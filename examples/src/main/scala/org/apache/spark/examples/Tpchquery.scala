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


object Tpchquery {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Tphch")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use tpch_external_1tb;")

    println("############################Query1###############################")
    time(spark.sql(" select  l_returnflag,  l_linestatus,  sum(l_quantity) as " +
      "sum_qty,  sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice" +
      " * (1 - l_discount)) as sum_disc_price,  sum(l_extendedprice * (1 - " +
      "l_discount) * (1 + l_tax)) as sum_charge,  avg(l_quantity) as avg_qty," +
      "  avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc,  " +
      "count(*) as count_order from  lineitem where  l_shipdate <= " +
      "'1998-09-16' group by  l_returnflag,  l_linestatus order by  " +
      "l_returnflag, l_linestatus"))

    println("############################Query2###############################")
    time(spark.sql("select  s_acctbal,  s_name,  n_name,  p_partkey,  p_mfgr,  " +
      "s_address,  s_phone,  s_comment from  part,  supplier,  partsupp,  " +
      "nation,  region,  q2_min_ps_supplycost where  p_partkey = ps_partkey  " +
      "and s_suppkey = ps_suppkey  and p_size = 37  and p_type like '%COPPER'" +
      "  and s_nationkey = n_nationkey  and n_regionkey = r_regionkey  and " +
      "r_name = 'EUROPE'  and ps_supplycost = min_ps_supplycost  and " +
      "p_partkey = min_p_partkey order by  s_acctbal desc,  n_name,  s_name, " +
      " p_partkey limit 100"))

    println("############################Query3###############################")
    time(spark.sql("select  l_orderkey,  sum(l_extendedprice * (1 - l_discount)) " +
      "as revenue,  o_orderdate,  o_shippriority from  customer,  orders,  " +
      "lineitem where  c_mktsegment = 'BUILDING'  and c_custkey = o_custkey  " +
      "and l_orderkey = o_orderkey  and o_orderdate < '1995-03-22'  and " +
      "l_shipdate > '1995-03-22' group by  l_orderkey,  o_orderdate,  " +
      "o_shippriority order by  revenue desc,  o_orderdate limit 10"))

    println("############################Query4###############################")
    time(spark.sql("select  o_orderpriority,  count(*) as order_count from  orders" +
      " as o where  o_orderdate >= '1996-05-01'  and o_orderdate < " +
      "'1996-08-01'  and exists (   select    *   from    lineitem   where   " +
      " l_orderkey = o.o_orderkey    and l_commitdate < l_receiptdate  ) " +
      "group by  o_orderpriority order by o_orderpriority"))

    println("############################Query5###############################")
    time(spark.sql("select  n_name,  sum(l_extendedprice * (1 - l_discount)) as " +
      "revenue from  customer,  orders,  lineitem,  supplier,  nation,  " +
      "region where  c_custkey = o_custkey  and l_orderkey = o_orderkey  and " +
      "l_suppkey = s_suppkey  and c_nationkey = s_nationkey  and s_nationkey " +
      "= n_nationkey  and n_regionkey = r_regionkey  and r_name = 'AFRICA'  " +
      "and o_orderdate >= '1993-01-01'  and o_orderdate < '1994-01-01' group " +
      "by  n_name order by revenue desc"))

    println("############################Query6###############################")
    time(spark.sql("select  sum(l_extendedprice * l_discount) as revenue from  " +
      "lineitem where  l_shipdate >= '1993-01-01'  and l_shipdate < " +
      "'1994-01-01'  and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and " +
      "l_quantity < 25"))

    println("############################Query7###############################")
    time(spark.sql("select  supp_nation,  cust_nation,  l_year,  sum(volume) as " +
      "revenue from  (   select    n1.n_name as supp_nation,    n2.n_name as " +
      "cust_nation,    year(l_shipdate) as l_year,    l_extendedprice * (1 - " +
      "l_discount) as volume   from    supplier,    lineitem,    orders,    " +
      "customer,    nation n1,    nation n2   where    s_suppkey = l_suppkey " +
      "   and o_orderkey = l_orderkey    and c_custkey = o_custkey    and " +
      "s_nationkey = n1.n_nationkey    and c_nationkey = n2.n_nationkey    " +
      "and (     (n1.n_name = 'KENYA' and n2.n_name = 'PERU')     or " +
      "(n1.n_name = 'PERU' and n2.n_name = 'KENYA')    )    and l_shipdate " +
      "between '1995-01-01' and '1996-12-31'  ) as shipping group by  " +
      "supp_nation,  cust_nation,  l_year order by  supp_nation,  " +
      "cust_nation, l_year"))

    println("############################Query8###############################")
    time(spark.sql("select  o_year,  sum(case   when nation = 'PERU' then volume  " +
      " else 0  end) / sum(volume) as mkt_share from  (   select    year" +
      "(o_orderdate) as o_year,    l_extendedprice * (1 - l_discount) as " +
      "volume,    n2.n_name as nation   from    part,    supplier,    " +
      "lineitem,    orders,    customer,    nation n1,    nation n2,    " +
      "region   where    p_partkey = l_partkey    and s_suppkey = l_suppkey  " +
      "  and l_orderkey = o_orderkey    and o_custkey = c_custkey    and " +
      "c_nationkey = n1.n_nationkey    and n1.n_regionkey = r_regionkey    " +
      "and r_name = 'AMERICA'    and s_nationkey = n2.n_nationkey    and " +
      "o_orderdate between '1995-01-01' and '1996-12-31'    and p_type = " +
      "'ECONOMY BURNISHED NICKEL'  ) as all_nations group by  o_year order by" +
      " o_year"))

    println("############################Query9###############################")
    time(spark.sql("select  nation,  o_year,  sum(amount) as sum_profit from  (   " +
      "select    n_name as nation,    year(o_orderdate) as o_year,    " +
      "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as " +
      "amount   from    part,    supplier,    lineitem,    partsupp,    " +
      "orders,    nation   where    s_suppkey = l_suppkey    and ps_suppkey =" +
      " l_suppkey    and ps_partkey = l_partkey    and p_partkey = l_partkey " +
      "   and o_orderkey = l_orderkey    and s_nationkey = n_nationkey    and" +
      " p_name like '%plum%'  ) as profit group by  nation,  o_year order by " +
      " nation, o_year desc"))

    println("############################Query10###############################")
    time(spark.sql("select  c_custkey,  c_name,  sum(l_extendedprice * (1 - " +
      "l_discount)) as revenue,  c_acctbal,  n_name,  c_address,  c_phone,  " +
      "c_comment from  customer,  orders,  lineitem,  nation where  c_custkey" +
      " = o_custkey  and l_orderkey = o_orderkey  and o_orderdate >= " +
      "'1993-07-01'  and o_orderdate < '1993-10-01'  and l_returnflag = 'R'  " +
      "and c_nationkey = n_nationkey group by  c_custkey,  c_name,  " +
      "c_acctbal,  c_phone,  n_name,  c_address,  c_comment order by  revenue" +
      " desc limit 20"))

    println("############################Query11###############################")
    time(spark.sql("select  ps_partkey, part_value as value from (  select   " +
      "ps_partkey,   part_value,   total_value  from   q11_part_tmp_cached " +
      "join q11_sum_tmp_cached ) a where  part_value > total_value * 0.0001 " +
      "order by value desc"))

    println("############################Query12###############################")
    time(spark.sql("select  l_shipmode,  sum(case   when o_orderpriority = " +
      "'1-URGENT'    or o_orderpriority = '2-HIGH'    then 1   else 0  end) " +
      "as high_line_count,  sum(case   when o_orderpriority <> '1-URGENT'    " +
      "and o_orderpriority <> '2-HIGH'    then 1   else 0  end) as " +
      "low_line_count from  orders,  lineitem where  o_orderkey = l_orderkey " +
      " and l_shipmode in ('REG AIR', 'MAIL')  and l_commitdate < " +
      "l_receiptdate  and l_shipdate < l_commitdate  and l_receiptdate >= " +
      "'1995-01-01'  and l_receiptdate < '1996-01-01' group by  l_shipmode " +
      "order by l_shipmode"))

    println("############################Query13###############################")
    time(spark.sql("select  c_count,  count(*) as custdist from  (   select    " +
      "c_custkey,    count(o_orderkey) as c_count   from    customer left " +
      "outer join orders on     c_custkey = o_custkey     and o_comment not " +
      "like '%unusual%accounts%'   group by    c_custkey  ) c_orders group by" +
      "  c_count order by  custdist desc, c_count desc"))

    println("############################Query14###############################")
    time(spark.sql(" select  100.00 * sum(case   when p_type like 'PROMO%'    then" +
      " l_extendedprice * (1 - l_discount)   else 0  end) / sum" +
      "(l_extendedprice * (1 - l_discount)) as promo_revenue from  lineitem, " +
      " part where  l_partkey = p_partkey  and l_shipdate >= '1995-08-01' and" +
      " l_shipdate < '1995-09-01'"))

    println("############################Query15###############################")
    time(spark.sql("create view revenue (supplier_no, total_revenue) as " +
      "select l_suppkey,  sum(l_extendedprice * (1 - l_discount))" +
      " from  lineitem where  l_shipdate >= date('1996-01-01')  and " +
      "l_shipdate < date('1996-04-01') group by l_suppkey"))
    time(spark.sql("select  s_suppkey,  s_name,  s_address,  s_phone, total_revenue " +
      "from  supplier, revenue where  s_suppkey = supplier_no  and total_revenue = " +
      "(select max(total_revenue) from revenue) order by s_suppkey"))
    spark.sql("drop view revenue")

    println("############################Query16###############################")
    time(spark.sql(" select  p_brand,  p_type,  p_size,  count(distinct " +
      "ps_suppkey) as supplier_cnt from  partsupp,  part where  p_partkey = " +
      "ps_partkey  and p_brand <> 'Brand#34'  and p_type not like 'ECONOMY " +
      "BRUSHED%'  and p_size in (22, 14, 27, 49, 21, 33, 35, 28)  and " +
      "partsupp.ps_suppkey not in (   select    s_suppkey   from    supplier " +
      "  where    s_comment like '%Customer%Complaints%'  ) group by  " +
      "p_brand,  p_type,  p_size order by  supplier_cnt desc,  p_brand,  " +
      "p_type, p_size"))

    println("############################Query17###############################")
    time(spark.sql("with q17_part as (   select p_partkey from part where     " +
      "p_brand = 'Brand#23'   and p_container = 'MED BOX' ), q17_avg as (   " +
      "select l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity" +
      "   from lineitem    where l_partkey IN (select p_partkey from " +
      "q17_part)   group by l_partkey ), q17_price as (   select   " +
      "l_quantity,   l_partkey,   l_extendedprice   from   lineitem   where  " +
      " l_partkey IN (select p_partkey from q17_part) ) select cast(sum" +
      "(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly from q17_avg, " +
      "q17_price where  t_partkey = l_partkey and l_quantity < t_avg_quantity"))

    println("############################Query18###############################")
    spark.sql("drop view q18_tmp_cached")
    spark.sql("drop table q18_large_volume_customer_cached")
    time(spark.sql("create view q18_tmp_cached as select  l_orderkey,  sum" +
      "(l_quantity) as t_sum_quantity from  lineitem where  l_orderkey is not" +
      " null group by l_orderkey"))
    time(spark.sql("create table q18_large_volume_customer_cached as select  " +
      "c_name,  c_custkey,  o_orderkey,  o_orderdate,  o_totalprice,  sum" +
      "(l_quantity) from  customer,  orders,  q18_tmp_cached t,  lineitem l " +
      "where  c_custkey = o_custkey  and o_orderkey = t.l_orderkey  and " +
      "o_orderkey is not null  and t.t_sum_quantity > 300  and o_orderkey = l" +
      ".l_orderkey  and l.l_orderkey is not null group by  c_name,  " +
      "c_custkey,  o_orderkey,  o_orderdate,  o_totalprice order by  " +
      "o_totalprice desc,  o_orderdate  limit 100"))

    println("############################Query19###############################")
    time(spark.sql("select  sum(l_extendedprice* (1 - l_discount)) as revenue from" +
      "  lineitem,  part where  (   p_partkey = l_partkey   and p_brand = " +
      "'Brand#32'   and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM " +
      "PKG')   and l_quantity >= 7 and l_quantity <= 7 + 10   and p_size " +
      "between 1 and 5   and l_shipmode in ('AIR', 'AIR REG')   and " +
      "l_shipinstruct = 'DELIVER IN PERSON'  )  or  (   p_partkey = l_partkey" +
      "   and p_brand = 'Brand#35'   and p_container in ('MED BAG', 'MED " +
      "BOX', 'MED PKG', 'MED PACK')   and l_quantity >= 15 and l_quantity <= " +
      "15 + 10   and p_size between 1 and 10   and l_shipmode in ('AIR', 'AIR" +
      " REG')   and l_shipinstruct = 'DELIVER IN PERSON'  )  or  (   " +
      "p_partkey = l_partkey   and p_brand = 'Brand#24'   and p_container in " +
      "('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')   and l_quantity >= 26 and " +
      "l_quantity <= 26 + 10   and p_size between 1 and 15   and l_shipmode " +
      "in ('AIR', 'AIR REG')   and l_shipinstruct = 'DELIVER IN PERSON' )"))

    println("############################Query20###############################")
    time(spark.sql("with tmp1 as (     select p_partkey from part where p_name " +
      "like 'forest%' ), tmp2 as (     select s_name, s_address, s_suppkey   " +
      "  from supplier, nation     where s_nationkey = n_nationkey     and " +
      "n_name = 'CANADA' ), tmp3 as (     select l_partkey, 0.5 * sum" +
      "(l_quantity) as sum_quantity, l_suppkey     from lineitem, tmp2     " +
      "where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'     " +
      "and l_suppkey = s_suppkey      group by l_partkey, l_suppkey ), tmp4 " +
      "as (     select ps_partkey, ps_suppkey, ps_availqty     from partsupp " +
      "     where ps_partkey IN (select p_partkey from tmp1) ), tmp5 as ( " +
      "select     ps_suppkey from     tmp4, tmp3 where     ps_partkey = " +
      "l_partkey     and ps_suppkey = l_suppkey     and ps_availqty > " +
      "sum_quantity ) select     s_name,     s_address from     supplier " +
      "where     s_suppkey IN (select ps_suppkey from tmp5) order by s_name"))

    println("############################Query21###############################")
    time(spark.sql("create temporary table l3 stored as orc as  select l_orderkey," +
      " count(distinct l_suppkey) as cntSupp from lineitem where " +
      "l_receiptdate > l_commitdate and l_orderkey is not null group by " +
      "l_orderkey having cntSupp = 1"))
    time(spark.sql("with location as ( select supplier.* from supplier, nation " +
      "where s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' ) select " +
      "s_name, count(*) as numwait from ( select li.l_suppkey, li.l_orderkey " +
      "from lineitem li join orders o on li.l_orderkey = o.o_orderkey and o" +
      ".o_orderstatus = 'F' join (select l_orderkey, count(distinct " +
      "l_suppkey) as cntSupp from lineitem group by l_orderkey ) l2 on li" +
      ".l_orderkey = l2.l_orderkey and li.l_receiptdate > li.l_commitdate and" +
      " l2.cntSupp > 1 ) l1 join l3 on l1.l_orderkey = l3.l_orderkey  join " +
      "location s on l1.l_suppkey = s.s_suppkey group by  s_name order by  " +
      "numwait desc,  s_name limit 100"))

    println("############################Query22###############################")
    spark.sql("drop view q22_customer_tmp_cached")
    spark.sql("drop view q22_customer_tmp1_cached")
    spark.sql("drop view q22_orders_tmp_cached")
    time(spark.sql("create view if not exists q22_customer_tmp_cached as select  " +
      "c_acctbal,  c_custkey,  substr(c_phone, 1, 2) as cntrycode from  " +
      "customer where  substr(c_phone, 1, 2) = '13' or  substr(c_phone, 1, 2)" +
      " = '31' or  substr(c_phone, 1, 2) = '23' or  substr(c_phone, 1, 2) = " +
      "'29' or  substr(c_phone, 1, 2) = '30' or  substr(c_phone, 1, 2) = '18'" +
      " or substr(c_phone, 1, 2) = '17'"))
    time(spark.sql("create view if not exists q22_customer_tmp1_cached as select  " +
      "avg(c_acctbal) as avg_acctbal from  q22_customer_tmp_cached where " +
      "c_acctbal > 0.00"))
    time(spark.sql("create view if not exists q22_orders_tmp_cached as select  " +
      "o_custkey from  orders group by o_custkey"))
    time(spark.sql("select  cntrycode,  count(1) as numcust,  sum(c_acctbal) as " +
      "totacctbal from (  select   cntrycode,   c_acctbal,   avg_acctbal  " +
      "from   q22_customer_tmp1_cached ct1 join (    select     cntrycode,   " +
      "  c_acctbal    from     q22_orders_tmp_cached ot     right outer join " +
      "q22_customer_tmp_cached ct     on ct.c_custkey = ot.o_custkey    where" +
      "     o_custkey is null   ) ct2 ) a where  c_acctbal > avg_acctbal " +
      "group by  cntrycode order by cntrycode"))

    def time[T](f: => T): T = {
      val start = System.nanoTime()
      val ret = f
      val end = System.nanoTime()
      // scalastyle:off println
      println(s"###########Time taken: ${(end - start) / 1000 / 1000} ms")
      val writer = new FileWriter("/opt/spark/Write.txt", true)
      writer.write(s"Time taken: ${(end - start) / 1000 / 1000} ms")
      writer.close()
      // scalastyle:on println
      ret
    }
  }
}


// scalastyle:on println
