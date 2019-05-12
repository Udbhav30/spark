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

import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object Sampleud {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("Sampleud")
      .getOrCreate()

   spark.sql("create database udbhav123")
    println("database udbhav created")
    spark.sql("use udbhav123")
    spark.sql("create table t1 (id int)")

    time(spark.sql("insert into t1 values(5)"))

    def time[T](f: => T): T = {
      val start = System.nanoTime()
      val ret = f
      val end = System.nanoTime()
      // scalastyle:off println
      println(s"###########Time taken: ${(end - start) / 1000 / 1000} ms")
      val writer = new FileWriter("/home/root1/Write.txt", true)
      writer.write(s"Time taken: ${(end - start) / 1000 / 1000} ms")
      writer.close()
      // scalastyle:on println
      ret
    }
    spark.sql("select * from t1").show
  }
}
// scalastyle:on println
