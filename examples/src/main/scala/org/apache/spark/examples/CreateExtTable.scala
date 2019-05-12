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
object CreateExtTable {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("Sampleud")
      .getOrCreate()

    spark.sql("create database if not exists tpch_external_1tb")
   spark.sql("use tpch_external_1tb")
    spark.sql("create external table lineitem \n(L_ORDERKEY BIGINT,\n " +
      "L_PARTKEY BIGINT,\n L_SUPPKEY BIGINT,\n L_LINENUMBER INT,\n L_QUANTITY" +
      " DOUBLE,\n L_EXTENDEDPRICE DOUBLE,\n L_DISCOUNT DOUBLE,\n L_TAX " +
      "DOUBLE,\n L_RETURNFLAG STRING,\n L_LINESTATUS STRING,\n L_SHIPDATE " +
      "STRING,\n L_COMMITDATE STRING,\n L_RECEIPTDATE STRING,\n " +
      "L_SHIPINSTRUCT STRING,\n L_SHIPMODE STRING,\n L_COMMENT STRING)\nROW " +
      "FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE " +
      "\nLOCATION 's3a://dli-performance/tpch/1024/lineitem'")

    spark.sql("create external table part (P_PARTKEY BIGINT,\n P_NAME STRING," +
      "\n P_MFGR STRING,\n P_BRAND STRING,\n P_TYPE STRING,\n P_SIZE INT,\n " +
      "P_CONTAINER STRING,\n P_RETAILPRICE DOUBLE,\n P_COMMENT STRING) \nROW " +
      "FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE " +
      "\nLOCATION 's3a://dli-performance/tpch/1024/part/'")

    spark.sql("create external table supplier (S_SUPPKEY BIGINT,\n S_NAME " +
      "STRING,\n S_ADDRESS STRING,\n S_NATIONKEY BIGINT,\n S_PHONE STRING,\n " +
      "S_ACCTBAL DOUBLE,\n S_COMMENT STRING) \nROW FORMAT DELIMITED FIELDS " +
      "TERMINATED BY '|' STORED AS TEXTFILE \nLOCATION " +
      "'s3a://dli-performance/tpch/1024/supplier/'")

    spark.sql("create external table partsupp (PS_PARTKEY BIGINT,\n " +
      "PS_SUPPKEY BIGINT,\n PS_AVAILQTY INT,\n PS_SUPPLYCOST DOUBLE,\n " +
      "PS_COMMENT STRING)\nROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "STORED AS TEXTFILE\nLOCATION's3a://dli-performance/tpch/1024/partsupp'")

    spark.sql("create external table nation (N_NATIONKEY BIGINT,\n N_NAME " +
      "STRING,\n N_REGIONKEY BIGINT,\n N_COMMENT STRING)\nROW FORMAT " +
      "DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE\nLOCATION " +
      "'s3a://dli-performance/tpch/1024/nation'")

    spark.sql("create external table region (R_REGIONKEY BIGINT,\n R_NAME " +
      "STRING,\n R_COMMENT STRING)\nROW FORMAT DELIMITED FIELDS TERMINATED BY" +
      " '|' STORED AS TEXTFILE\nLOCATION " +
      "'s3a://dli-performance/tpch/1024/region'")

    spark.sql("create external table customer (C_CUSTKEY BIGINT,\n C_NAME " +
      "STRING,\n C_ADDRESS STRING,\n C_NATIONKEY BIGINT,\n C_PHONE STRING,\n " +
      "C_ACCTBAL DOUBLE,\n C_MKTSEGMENT STRING,\n C_COMMENT STRING)\nROW " +
      "FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE\nLOCATION" +
      " 's3a://dli-performance/tpch/1024/customer'")

    spark.sql("create external table orders (O_ORDERKEY BIGINT,\n O_CUSTKEY " +
      "BIGINT,\n O_ORDERSTATUS STRING,\n O_TOTALPRICE DOUBLE,\n O_ORDERDATE " +
      "STRING,\n O_ORDERPRIORITY STRING,\n O_CLERK STRING,\n O_SHIPPRIORITY " +
      "INT,\n O_COMMENT STRING)\nROW FORMAT DELIMITED FIELDS TERMINATED BY " +
      "'|' STORED AS TEXTFILE\nLOCATION 's3a://dli-performance/tpch/1024/orders'")

    println("Successfully created external table#####################")
    spark.stop()
  }
}
// scalastyle:on println
