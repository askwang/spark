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

package org.apache.spark.sql

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ResetSystemProperties

@ExtendedSQLTest
class AskwangSQLQuerySuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
    with ResetSystemProperties {

  setupTestData()

  test("select timestamp field by where") {
    withTable("tb") {
      sql("CREATE TABLE `tb`(i INT, dt TIMESTAMP) USING parquet")
      val ds = sql("INSERT INTO `tb` VALUES (1,cast(\"2024-04-11 11:01:00\" as Timestamp))")

      spark.conf.set("spark.sql.planChangeLog.level", "WARN")
      println("=================================================")
      val data = sql("SELECT * FROM `tb` where dt ='2024-04-11 11:01:00' ")
      println("=================================================")

      spark.conf.set("spark.sql.planChangeLog.level", "INFO")

      println(data.queryExecution)
      println(data.show())
      println(data.explain(true))
    }
  }
}