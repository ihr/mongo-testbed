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
package org.ingini.mongo.testbed;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import org.fest.assertions.Assertions;
import org.ingini.monogo.testbed.MongoManager;
import org.ingini.monogo.testbed.annotation.MongoTestBedCollection;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests embedded mongo
 */
public class TestMongoEmbeddedFlapdoodle {

    @ClassRule
    public static MongoManager mongoManager = MongoManager.mongoFlapdoodle(9810);

    @MongoTestBedCollection(name="test_collection", location = "test_collection.json")
    public static DBCollection collection;



    @Test
    @Ignore
    public void shouldHaveAllDataInMongo() {
        //GIVEN

        //WHEN
        DBCursor dbObjects = collection.find();

        //THEN
        Assertions.assertThat((Iterable<?>) dbObjects).hasSize(4);
    }
}
