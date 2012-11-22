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
package org.ingini.monogo.testbed.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use this annotation on a public static field (class instance variable).
 * Mongo-TestBed will insert the JSON file specified at {@code location} parameter in mongo database under name
 * given through the {@code name} parameter
 * <p>Example:</p>
 * <p>@MongoTestBedCollection(name = "test_collection", location = "test_collection.json")</p>
 * <p>public static DBCollection collection;</p>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface MongoTestBedCollection {

    String name();

    String location();

}
