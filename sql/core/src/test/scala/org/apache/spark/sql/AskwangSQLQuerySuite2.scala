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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ResetSystemProperties

/**
 * sparksql 课程测试
 */
@ExtendedSQLTest
class AskwangSQLQuerySuite2 extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
    with ResetSystemProperties {

  setupTestData()

  test("init data") {
    spark.conf.set("spark.sql.cli.print.header", "true")
    sql("select * from testdata2").show
  }

  test("inner join") {
    withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "WARN") {
      val analyzedDF = sql("select tmp1.a,tmp1.b from testdata2 tmp1 join testdata2 tmp2 on tmp1.a=tmp2.a and tmp1.b<1 and tmp2.b>3")
      // val analyzedDF = sql("select  tmp.A from (select A,B from testdata2) tmp ")
      // val analyzedDF = sql("select  a1,(b+1) as b1   from (select (a+1) as a1 ,b from testdata2) tmp ")
      // val analyzedDF = sql("select * from testdata2 ")

      analyzedDF.explain(true)
      // showQueryExecutionPlanInfo(analyzedDF)
    }
  }

  def showQueryExecutionPlanInfo(analyzedDF: DataFrame): Unit = {
    val ana = analyzedDF.queryExecution.analyzed
    println("== Analyzed Logical Plan ==" )
    println(ana)
    // println( ana.prettyJson)
    println("== Optimized Logical Plan ==" )
    val opt = analyzedDF.queryExecution.optimizedPlan
    println(opt)
    // println( opt.prettyJson)
    println("== Physical Plan ==" )
    println(analyzedDF.queryExecution.sparkPlan)
    println("== executedPlan ==" )
    println(analyzedDF.queryExecution.executedPlan)
  }
}