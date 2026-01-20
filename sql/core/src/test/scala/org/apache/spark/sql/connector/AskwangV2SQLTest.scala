/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector

/**
 * copy from [[DataSourceV2SQLSuite]]
 */
class AskwangV2SQLTest
  extends DataSourceV2SQLSuite
    {

      override protected val catalogAndNamespace = "testv2filter.ns1.ns2."


  test("insertInto") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id int, data string) USING foo partitioned by (day string, hour string)")

      sql(s"INSERT INTO $t1 VALUES(1, 'a', '2026-01-01', '01')")

      sql(s"show partitions $t1").show(false)
    }
  }

}