/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** The famous word count example that shows a minimal Flink SQL job in batch execution mode. */
public final class KafkaExample {

    public static void main(String[] args) throws Exception {

        // set up the Table API
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        // execute a Flink SQL job and print the result locally

        tableEnv.executeSql("CREATE TABLE trade (\n" +
                "  `id` VARCHAR(20),\n" +
                "  `name` VARCHAR(20),\n" +
                "  `country` VARCHAR(20),\n" +
                "  `age` INT,\n" +
                "  `proctime` AS PROCTIME()\n" +
                ")\n" +
                "COMMENT ''\n" +
                "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = '10.194.188.93:30355',\n" +
                "  'properties.group.id' = 'my-group',\n" +
                "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"brokerUser\" password=\"brokerPassword\";',\n" +
                "  'properties.sasl.mechanism' = 'SCRAM-SHA-256',\n" +
                "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                "  'property-version' = 'universal',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'topic' = 'trade1',\n" +
                "  'value.format' = 'csv'\n" +
                ");") ;

        tableEnv.executeSql("CREATE TABLE g3 (\n" +
                "  id VARCHAR(4) PRIMARY KEY NOT ENFORCED,\n" +
                "  name VARCHAR(3),\n" +
                "  country VARCHAR(2),\n" +
                "  age INT\n" +
                ")\n" +
                "COMMENT ''\n" +
                "WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1'\n" +
                ");\n");

        tableEnv.executeSql("insert into trade select * from g3");
    }
}
