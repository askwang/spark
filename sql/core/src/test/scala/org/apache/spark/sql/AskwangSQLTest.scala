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
 * copy from [[SQLQuerySuite]]
 */
@ExtendedSQLTest
class AskwangSQLTest extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
    with ResetSystemProperties {
  import testImplicits._

  setupTestData()

  // EliminateOuterJoin， PushPredicateThroughJoin
  test("[askwang-test] test filter + join case1") {
    withSQLConf() {
      println("====testdata2======")
      sql("select * from testdata2").show(false)

      println("========== inner join =================")
      // inner join
      sql("select d1.a,d1.b from testdata2 d1 inner join testdata2 d2 on d1.a=d2.a where d1.b < 1 and d2.b > 3").explain(true)

      println("========== left join => inner join =================")
      // left join => inner join
      // 下面两个 case 效果一样的，右表有 filter，都会转换为 inner join
      sql("select d1.a,d1.b from testdata2 d1 left join testdata2 d2 on d1.a=d2.a where d1.b < 1 and d2.b > 3").explain(true)
      sql("select d1.a,d1.b from testdata2 d1 left join testdata2 d2 on d1.a=d2.a where d2.b > 3").explain(true)

      println("========== left join =================")
      // left join
      // askwang-done: d2 scan 没有 Project[a#271] 节点进行列裁剪优化
      // A: SerializeFromObject scan 的时候就只 scan 了列 a，把列 b 已经裁剪掉了
      // 上面的 case 有 Project 节点，是因为 filter 过滤会用到 b 列，Scan(a, b) => Filter(b > 3) => Project(a)
      sql("select d1.a,d1.b from testdata2 d1 left join testdata2 d2 on d1.a=d2.a where d1.b < 1").explain(true)

      println("=========== right join => inner join ================")
      // right join => inner join
      sql("select d1.a,d1.b from testdata2 d1 right join testdata2 d2 on d1.a=d2.a where d1.b < 1").explain(true)

      println("============ right join ===============")
      // right join
      sql("select d1.a,d1.b from testdata2 d1 right join testdata2 d2 on d1.a=d2.a where d2.b > 3").explain(true)
    }
  }

  test("[askwang-test] filter + join case2") {
    withSQLConf() {
      sql("create table t1 (id1 int, name string) using parquet")
      sql("create table t2 (id2 int, age int) using parquet")

      sql("insert into t1 values (1,'a'),(2,'b'),(5,'e')")
      sql("insert into t2 values (1,11),(2,22),(4,33)")

      val query = sql("select t1.id1, t1.name,t2.id2,t2.age from t1 left join t2 on t1.id1 = t2.id2 where t1.id1 < 3")
      query.show()
      query.explain(true)
    }
  }


  test("[askwang-test] join + on case") {
    withSQLConf() {
      sql("create table t1 (id1 int, name string) using parquet")
      sql("create table t2 (id2 int, age int) using parquet")

      sql("insert into t1 values (1,'a'),(2,'b'),(5,'e')")
      sql("insert into t2 values (1,11),(2,22),(4,33)")

      // t1.id1 < 3 表示 t1 的 id = 1 和 id = 2 都匹配
      // t2.id2 < 2 表示 t2 的 id = 1 匹配，所以最终只有 id = 1 完成匹配
      val query = sql("select t1.id1, t1.name,t2.id2,t2.age from t1 left join t2 on t1.id1 = t2.id2 and t1.id1 < 3 and t2.id2 < 2")
      query.show()
      query.explain(true)
    }
  }

  test("[askwang-test] create table column with default value") {
    withSQLConf() {
      spark.sql("create table t (id int, name string) using parquet")
      spark.sql("insert into t values (1, 'a')")

      spark.sql("alter table t add column c int default 100")

      spark.sql("select * from t").show(false)
    }
  }


  test("[askwang-test] xxx") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      sql("create table t1 (id1 int, name string) using parquet")
      sql("create table t2 (id2 int, age int) using parquet")

      sql("insert into t1 values (1,'a'),(2,'b'),(5,'e')")
      sql("insert into t2 values (1,11),(2,22),(5, cast(null as int))")

      val query = sql("select t1.id1, t1.name,t2.id2,t2.age from t1 left join t2 on t1.id1 = t2.id2 where t2.id2 < 5")
      query.show()
      query.explain(true)
    }
  }

  test("left outer join") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        sql("SELECT * FROM uppercasedata LEFT OUTER JOIN lowercasedata ON n = N"),
        Row(1, "A", 1, "a") ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)
    }
  }

  test("create temp view") {
    // spark.sql.planChangeLog.rules
    withSQLConf("spark.sql.planChangeLog.level" -> "TRACE") {
      withTable("t") {
        Seq(2, 3, 1).toDF("c1").write.format("parquet").saveAsTable("t")

        // SqlBaseParser.g4 语法文件
        // createView: CreateViewCommand
        // createTempViewUsing: CreateTempViewUsing
//        sql("create temp view v1 as select c1 from t").explain(true)

        println("===")
      }
    }
  }

  test("alter table drop partition") {
    sql(
      s"""
         |CREATE TABLE T (id STRING, appid string) using parquet
         | PARTITIONED BY (day string, hour string)
         |""".stripMargin)

    sql(" insert into T values ('1', '004', '2026-01-15', '15');")
    sql(" insert into T values ('1', '004', '2026-01-15', '16');")
    sql(" insert into T values ('1', '003', '2026-01-16', '16');")
    sql(" insert into T values ('2', '004', '2026-01-16', '17');")
    sql("show partitions T").show(false)

    val df = sql("alter table T drop partition (day='2026-01-15', hour = '16')")

    sql("show partitions T").show(false)

  }
}
