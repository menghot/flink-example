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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * The famous word count example that shows a minimal Flink SQL job in batch execution mode.
 */
public final class IcebergHiveCatalogExample {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();

        //assign more memory to avoid "group by" cause insufficient memory issue
        settings.getConfiguration().set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(256));

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE CATALOG ice WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://10.194.188.93:9083',\n" +
                "  'clients'='5',\n" +
                "  'hive-conf-dir'='./hive-conf',\n" +
                "  'warehouse'='s3a://data/warehouse'\n" +
                ");");

        tableEnv.executeSql("CREATE CATALOG ice2 WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://10.194.186.214:9083',\n" +
                "  'clients'='5',\n" +
                "  'warehouse'='hdfs://user/hive/warehouse'\n" +
                ");");

        tableEnv.executeSql("CREATE CATALOG pg WITH(\n" +
                "    'type' = 'jdbc',\n" +
                "    'default-database' = 'hms',\n" +
                "    'username' = 'postgres',\n" +
                "    'password' = 'thinker',\n" +
                "    'base-url' = 'jdbc:postgresql://10.194.188.93'\n" +
                ");");

        tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='./warehouse',\n" +
                "  'property-version'='1'\n" +
                ");");

//        tableEnv.executeSql("create database hadoop_catalog.ods");
//        tableEnv.executeSql("CREATE TABLE hadoop_catalog.ods.sample2 (\n" +
//                "    id  INT ,\n" +
//                "    name varchar(200) ,\n" +
//                "    data varchar(200) ,\n" +
//                "    PRIMARY KEY(id) NOT ENFORCED\n" +
//                ") with ('format-version'='2', 'write.upsert.enabled'='true');");

        Arrays.stream(tableEnv.listCatalogs()).iterator().forEachRemaining(System.out::println);

        tableEnv.useCatalog("ice2");

        Arrays.stream(tableEnv.listDatabases()).iterator().forEachRemaining(System.out::println);

        tableEnv.useDatabase("simon");


        System.out.println("-------------- catalogs end");

        System.out.println("-------------- databases end");

        Arrays.stream(tableEnv.listTables()).iterator().forEachRemaining(System.out::println);
        System.out.println("-------------- tables end");

        tableEnv.executeSql("show create table ice.cracn.trade").print();
        tableEnv.executeSql("select * from ice.cracn.trade order by id desc").print();
        tableEnv.executeSql("select count(*) as cnt from ice.cracn.trade").print();

//        tableEnv.executeSql("CREATE TABLE ice.cracn.sample2 (\n" +
//                "    id  INT ,\n" +
//                "    name varchar(200) ,\n" +
//                "    data varchar(200) ,\n" +
//                "    PRIMARY KEY(id) NOT ENFORCED\n" +
//                ") with ('format-version'='2', 'write.upsert.enabled'='true');");

        tableEnv.executeSql("show create table ice.cracn.sample2").print();
        tableEnv.executeSql("select * from ice.cracn.sample2").print();

        //for(int i = 0 ; i< 100; i++) {
        tableEnv.executeSql("insert into ice.cracn.sample2 values(1, 'simon', 'v1')");

        long start = System.currentTimeMillis();

        tableEnv.executeSql("select count(*) from ice2.simon.ice_person_20w;").print();

        tableEnv.executeSql("select count(distinct(person_id)) cnt from ice2.simon.ice_person_20w ").print();

        tableEnv.executeSql("select person_id, count(b.id) from ice2.simon.ice_person_20w a left join ice.cracn.sample2 b on a.person_id = b.id group by a.person_id limit 10").print();

        System.out.println(System.currentTimeMillis() - start);
    }
}
