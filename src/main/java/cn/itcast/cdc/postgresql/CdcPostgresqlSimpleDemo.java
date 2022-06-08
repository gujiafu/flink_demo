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

package cn.itcast.cdc.postgresql;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 *
 */
public class CdcPostgresqlSimpleDemo {

    public static void main(String[] args, Properties properties1) throws Exception {

        Properties dbzProperties = new Properties();
        properties1.setProperty("heartbeat.interval.ms", String.valueOf(5000));

        DebeziumSourceFunction<String> sourceFunction =
        PostgreSQLSource.<String>builder()
            .hostname("192.168.88.166")
            .port(1433)
            .database("TestDB")
            .tableList("dbo.Inventory")
            .username("SA")
            .password("SqlServer2017")
            .debeziumProperties(dbzProperties)
//            .startupOptions(StartupOptions.latest())
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
