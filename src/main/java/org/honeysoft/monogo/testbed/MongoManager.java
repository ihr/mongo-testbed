/*
 * Copyright (c) 2012 Ivan Hristov <hristov.iv@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.honeysoft.monogo.testbed;

import com.mongodb.*;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONCallback;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.honeysoft.monogo.testbed.annotation.MongoCollection;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.CharBuffer;
import java.util.concurrent.*;

import static org.fest.reflect.core.Reflection.*;

/**
 * A mongo DB manager to be used as JUnit rule
 */
public class MongoManager implements TestRule {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String MONOGO_TESTBED_DB = "monogo-testbed-db";
    public static final String MONGODB_TESTBED_INSTANCE = "mongodb-testbed-instance";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9819;

    private static final int BUFFER_SIZE = 8192; // 8K bytes / 2 bytes = 4K characters
    private static final int EOF = -1;
    private static final int START = 0;

    private Process process;

    private MongodExecutable mongodExe;
    private MongodProcess mongod;
    private DB mongoDB;
    private Mongo mongo;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private Future<?> externalMongoThread;

    public MongoManager() {

        logger.debug("Starting Mongo-TestBed coordinator ...");
        try {

            MongodStarter runtime = MongodStarter.getDefaultInstance();
            mongodExe = runtime.prepare(new MongodConfig(Version.V2_2_0, DEFAULT_PORT, Network.localhostIsIPv6()));
            mongod = mongodExe.start();

            mongo = new Mongo(DEFAULT_HOST, DEFAULT_PORT);
            logger.debug("Mongo TestBed process {} created.", MONGODB_TESTBED_INSTANCE);
            mongoDB = mongo.getDB(MONOGO_TESTBED_DB);
            logger.debug("Mongo TestBed database {} created.", MONOGO_TESTBED_DB);
        } catch (UnknownHostException e) {
            logger.error("Unable to start mongo due to an exception!", e);
        } catch (IOException e) {
            logger.error("Unable to start mongo due to an exception!", e);
        }
    }

    /**
     * Use this constructor if you want to redirect the MongoDB output to a specific file.
     *
     * @param command
     * @param dbpath  directory for datafiles
     */
    public MongoManager(final String command, final String dbpath) {
        try {

            externalMongoThread = executorService.submit(new Runnable() { //TODO externalize into a separate class
                @Override
                public void run() {
                    try {
                        ProcessBuilder processBuilder = new ProcessBuilder(command, "--port", String.valueOf(DEFAULT_PORT), "--dbpath", dbpath);
                        processBuilder.directory(new File("target"));
                        processBuilder.redirectErrorStream(true);
                        process = processBuilder.start();

                        BufferedReader bufferedInputStream = new BufferedReader(new InputStreamReader(process.getInputStream()));

                        String line;
                        while ((line = bufferedInputStream.readLine()) != null && !Thread.interrupted()) {
                            System.out.println("Mongo DB: " + line);
                        }

                    } catch (IOException e) {
                        logger.error("Could not start external mongo process due to an exception!", e);
                    }
                }
            });

            mongo = new Mongo(DEFAULT_HOST, DEFAULT_PORT);
            logger.debug("Mongo TestBed process {} created.", MONGODB_TESTBED_INSTANCE);
            mongoDB = mongo.getDB(MONOGO_TESTBED_DB);
            logger.debug("Mongo TestBed database {} created.", MONOGO_TESTBED_DB);
        } catch (IOException e) {
            logger.error("Could not start external mongo process due to an exception!", e);
        }
    }

    @Override
    public Statement apply(final Statement base, Description description) {


        Field[] declaredFields = description.getTestClass().getDeclaredFields();
        for (Field field : declaredFields) {
            Annotation[] annotations = field.getDeclaredAnnotations();
            for (Annotation annotation : annotations) {
                if (annotation instanceof MongoCollection) {
                    String location = ((MongoCollection) annotation).location();
                    String name = ((MongoCollection) annotation).name();
                    if (mongoDB.collectionExists(name)) {
                        logger.debug("Dropping already existing mongo collection {}", name);
                        mongoDB.getCollection(name).drop();
                    }
                    DBCollection collection = mongoDB.createCollection(name, new BasicDBObject());
                    fill(collection, location);
                    field(field.getName()).ofType(DBCollection.class).in(description.getTestClass()).set(collection);
                }
            }
        }

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {
                    logger.debug("Stopping Mongo TestBed ...");
                    stop();

                }
            }
        };
    }

    public void stop() {
        //TODO all these ifs should be externalized to a strategy map
        if (process != null) {
            logger.debug("Destroying external mongo process ...");
            process.destroy();
            executorService.shutdownNow();
        }

        if (mongod != null) {
            logger.debug("Stopping mongod ...");
            mongod.stop();
        }

        if (mongodExe != null) {
            logger.debug("Cleaning-up mongo executable ...");
            mongodExe.cleanup();
        }
    }

    private void fill(DBCollection collection, String collectionContentFilePath) {
        StringBuilder stringBuilder = new StringBuilder();

        try {
            InputStreamReader inputStreamReader = new InputStreamReader( //
                    CollectionManager.class.getClassLoader().getResourceAsStream(collectionContentFilePath), "UTF-8");
            CharBuffer buf = CharBuffer.allocate(BUFFER_SIZE);
            for (int read = inputStreamReader.read(buf); read != EOF; read = inputStreamReader.read(buf)) {
                buf.flip();
                stringBuilder.append(buf, START, read);

            }
        } catch (IOException e) {
            logger.error("Unable to read input stream due to an exception!", e);
            throw new IllegalStateException(e);
        }

        BasicDBList parse = (BasicDBList) JSON.parse(stringBuilder.toString(), new MongoIdTransformerJSONCallback());
        collection.insert(parse.toArray(new DBObject[parse.size()]));
    }

    private static class MongoIdTransformerJSONCallback extends JSONCallback {

        private static final String MONGO_ID_KEY = "_id";

        @Override
        public Object objectDone() {
            BSONObject b = (BSONObject) super.objectDone();
            if (b.containsField(MONGO_ID_KEY) && b.get(MONGO_ID_KEY) instanceof String) {
                b.put(MONGO_ID_KEY, new ObjectId(b.get(MONGO_ID_KEY).toString()));
            }
            return b;
        }
    }
}
