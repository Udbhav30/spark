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

// scalastyle:off println
package org.apache.spark.examples

import java.io.FileWriter

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object Createtable {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("Sampleud")
      .getOrCreate()

        spark.sql("create database if not exists tpch_parquet_1tb")
        spark.sql("use tpch_parquet_1tb")
    time(spark.sql("create table lineitem STORED AS PARQUET AS (SELECT * FROM" +
      " tpch_external_1tb.lineitem)"))

    time(spark.sql("create table part STORED AS PARQUET AS (SELECT * FROM " +
      "tpch_external_1tb.part)"))

    time(spark.sql("create table supplier STORED AS PARQUET AS (SELECT * FROM" +
      " tpch_external_1tb.supplier)"))

    time(spark.sql("create table partsupp STORED AS PARQUET AS (SELECT * FROM" +
      " tpch_external_1tb.partsupp)"))

    time(spark.sql("create table nation STORED AS PARQUET AS (SELECT * FROM " +
      "tpch_external_1tb.nation)"))

    time(spark.sql("create table region STORED AS PARQUET AS (SELECT * FROM " +
      "tpch_external_1tb.region)"))

    time(spark.sql("create table customer STORED AS PARQUET AS (SELECT * FROM" +
      " tpch_external_1tb.customer)"))

    time(spark.sql("create table orders STORED AS PARQUET AS (SELECT * FROM " +
      "tpch_external_1tb.orders)"))

    def time[T](f: => T): T = {
      val start = System.nanoTime()
      val ret = f
      val end = System.nanoTime()
      // scalastyle:off println
      println(s"###########Time taken: ${(end - start) / 1000 / 1000} ms")
      val writer = new FileWriter("/opt/spark/Write.txt", true)
      writer.write(s"Time taken: ${(end - start) / 1000 / 1000} ms")
      writer.close()
      ret
    }

    println("Successfully created parquet table#####################")

    spark.stop()
  }
}

// scalastyle:on println
