/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api("com.google.guava:guava")
    api("org.slf4j:slf4j-api")

    implementation("org.apache.calcite.avatica:avatica-core")
    // https://mvnrepository.com/artifact/org.lightcouch/lightcouch
    implementation("org.lightcouch:lightcouch:0.2.0")
    // https://mvnrepository.com/artifact/com.googlecode.json-simple/json-simple
    implementation("com.googlecode.json-simple:json-simple:1.1.1")

    testImplementation(project(":testkit"))
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
}

