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
package org.ingini.monogo.testbed;

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
import org.ingini.monogo.testbed.annotation.MongoTestBedCollection;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.CharBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.fest.reflect.core.Reflection.field;

/**
 * A mongo DB manager to be used as JUnit rule
 */
public class MongoManager implements TestRule {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String MONOGO_TESTBED_DB = "monogo_testbed_db";
    public static final String MONGODB_TESTBED_INSTANCE = "mongodb_testbed_instance";
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


    public static MongoManager mongoFlapdoodle() {
        return new MongoManager(DEFAULT_PORT);
    }

    public static MongoManager mongoFlapdoodle(int port) {
        return new MongoManager(port);
    }

    private MongoManager(int port) {

        logger.debug("Starting Mongo-TestBed coordinator ...");
        try {

            MongodStarter runtime = MongodStarter.getDefaultInstance();
            mongodExe = runtime.prepare(new MongodConfig(Version.V2_2_0, port, Network.localhostIsIPv6()));
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

    public static MongoManager mongoStartLocal(String command, final String dbpath) {
        return new MongoManager(command, dbpath);
    }

    /**
     * Use this constructor if you want to run mongoDB via system command
     *
     * @param command
     * @param dbpath  directory for datafiles
     */
    private MongoManager(final String command, final String dbpath) {
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

    /**
     * Use this method to connect to a running instance of MongoDB
     *
     * @param uri pointing to the runing MongoDB instance
     * @return
     * @throws IllegalStateException in case of difficulties while connecting
     */
    public static MongoManager mongoConnect(String uri) {
        return new MongoManager(uri);
    }

    /**
     * Tries to connect to a given running MongoDB instance at {@code uri}
     * <p>Example URIs:</p>
     *      *) mongodb://127.0.0.1:27017 - connecting to 127.0.0.1 @ port 27017 without authenticating
     *      *) mongodb://john:doe@127.0.0.1:27017 - connecting to 127.0.0.1 @ port 27017 with username: 'john' and password: 'doe'
     *      *) mongodb://john:@127.0.0.1:27017 - connecting to 127.0.0.1 @ port 27017 with username: 'john' and empty password
     * @param uri
     * @throws IllegalStateException in case of difficulties while connecting
     */
    private MongoManager(String uri) {
        MongoURI mongoURI = new MongoURI(uri);
        try {
            this.mongo = mongoURI.connect();
        } catch (UnknownHostException e) {
            logger.error("Could not connect to {} due to an exception!", uri, e);
            throw new IllegalStateException(e);
        }
        this.mongoDB = mongo.getDB(MONOGO_TESTBED_DB);
        if (mongoURI.getUsername() != null) {
            this.mongoDB.authenticate(mongoURI.getUsername(), mongoURI.getPassword());
        }
    }

    @Override
    public Statement apply(final Statement base, Description description) {


        Field[] declaredFields = description.getTestClass().getDeclaredFields();
        for (Field field : declaredFields) {
            if (field.isAnnotationPresent(MongoTestBedCollection.class)) {
                MongoTestBedCollection annotation = field.getAnnotation(MongoTestBedCollection.class);
                String location = annotation.location();
                String name = annotation.name();
                if (mongoDB.collectionExists(name)) {
                    logger.debug("Dropping already existing mongo collection {}", name);
                    mongoDB.getCollection(name).drop();
                }
                DBCollection collection = mongoDB.createCollection(name, new BasicDBObject());
                fill(collection, location);
                field(field.getName()).ofType(DBCollection.class).in(description.getTestClass()).set(collection);
            }

            if (field.isAnnotationPresent(Inject.class)) {
                if (field.getType() == Mongo.class) {
                    field(field.getName()).ofType(Mongo.class).in(description.getTestClass()).set(mongo);
                } else if (field.getType() == DB.class) {
                    field(field.getName()).ofType(DB.class).in(description.getTestClass()).set(mongoDB);
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
