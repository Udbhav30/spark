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

import scala.math.random

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object Test {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("Sampleud")
      .getOrCreate()

    spark.sql("show databases").show()


    def time[T](f: => T): T = {
      val start = System.nanoTime()
      val ret = f
      val end = System.nanoTime()
      // scalastyle:off println
      println(s"###########Time taken: ${(end - start) / 1000 / 1000} ms")
      // scalastyle:on println
      ret
    }
  }
}

// scalastyle:on println
