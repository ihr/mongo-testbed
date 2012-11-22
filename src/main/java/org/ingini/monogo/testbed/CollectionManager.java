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

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONCallback;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.CharBuffer;

/**
 * A collection manager class to be applied as a JUnit rule
 */
public class CollectionManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final int BUFFER_SIZE = 8192; // 8K bytes / 2 bytes = 4K characters
    private static final int EOF = -1;
    private static final int START = 0;
    private DBCollection dbCollection;

    public CollectionManager(String collectionName, String collectionContentFilePath) {
//        dbCollection = Coordinator.getOrCreate().getOrCreateCollection(collectionName);
        String jsonAsString = toString(CollectionManager.class.getClassLoader().getResourceAsStream(collectionContentFilePath));
        BasicDBList parse = (BasicDBList) JSON.parse(jsonAsString, new MongoIdTransformerJSONCallback());
        dbCollection.insert(parse.toArray(new DBObject[parse.size()]));
    }


    public DBCollection getCollection() {
        return dbCollection;
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

    private String toString(InputStream resourceAsStream) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream, "UTF-8");
            StringBuilder stringBuilder = new StringBuilder();
            CharBuffer buf = CharBuffer.allocate(BUFFER_SIZE);
            for (int read = inputStreamReader.read(buf); read != EOF; read = inputStreamReader.read(buf)) {
                buf.flip();
                stringBuilder.append(buf, START, read);

            }
            return stringBuilder.toString();
        } catch (IOException e) {
            logger.error("Unable to read input stream due to an exception!", e);
            throw new IllegalStateException(e);
        }
    }

}
