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
package org.honeysoft.mongo.testbed;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import org.fest.assertions.Assertions;
import org.honeysoft.monogo.testbed.MongoManager;
import org.honeysoft.monogo.testbed.annotation.MongoCollection;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests coordination between {@link org.honeysoft.monogo.testbed.MongoManager} and {@link org.honeysoft.monogo.testbed.CollectionManager}
 */
public class TestMongoExternal {

    @ClassRule
    public static MongoManager mongoManager = new MongoManager();

    @MongoCollection(name="test_collection", location = "test_collection.json")
    public static DBCollection collection;

    @Test
    public void shouldHaveAllDataInMongo() {
        //GIVEN

        //WHEN
        DBCursor dbObjects = collection.find();

        //THEN
        Assertions.assertThat((Iterable<?>) dbObjects).hasSize(4);
    }
}
