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

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object TpcdsCreateExt {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("Sampleud")
      .getOrCreate()

    spark.sql("create database if not exists tpcds_external_10tb")
    spark.sql("use tpcds_external_10tb")

    spark.sql("create external table call_center(\n      cc_call_center_sk   " +
      "      bigint               \n,     cc_call_center_id         string   " +
      "           \n,     cc_rec_start_date        string                    " +
      "     \n,     cc_rec_end_date          string                         " +
      "\n,     cc_closed_date_sk         bigint                       \n,    " +
      " cc_open_date_sk           bigint                       \n,     " +
      "cc_name                   string                   \n,     cc_class   " +
      "               string                   \n,     cc_employees          " +
      "    int                       \n,     cc_sq_ft                  int   " +
      "                    \n,     cc_hours                  string          " +
      "            \n,     cc_manager                string                  " +
      " \n,     cc_mkt_id                 int                       \n,     " +
      "cc_mkt_class              string                      \n,     " +
      "cc_mkt_desc               string                  \n,     " +
      "cc_market_manager         string                   \n,     cc_division" +
      "               int                       \n,     cc_division_name     " +
      "     string                   \n,     cc_company                int   " +
      "                    \n,     cc_company_name           string          " +
      "            \n,     cc_street_number          string                  " +
      "    \n,     cc_street_name            string                   \n,    " +
      " cc_street_type            string                      \n,     " +
      "cc_suite_number           string                      \n,     cc_city " +
      "                  string                   \n,     cc_county          " +
      "       string                   \n,     cc_state                  " +
      "string                       \n,     cc_zip                    string " +
      "                     \n,     cc_country                string         " +
      "          \n,     cc_gmt_offset             double                  " +
      "\n,     cc_tax_percentage         double\n)\nrow format delimited " +
      "fields terminated by '|' \nlocation " +
      "'s3a://dli-performance/tpcds/10240/call_center'")

    spark.sql("create external table catalog_page(\n      cp_catalog_page_sk " +
      "       bigint               \n,     cp_catalog_page_id        string  " +
      "            \n,     cp_start_date_sk          bigint                  " +
      "     \n,     cp_end_date_sk            bigint                       " +
      "\n,     cp_department             string                   \n,     " +
      "cp_catalog_number         int                       \n,     " +
      "cp_catalog_page_number    int                       \n,     " +
      "cp_description            string                  \n,     cp_type     " +
      "              string\n)\nrow format delimited fields terminated by '|'" +
      " \nlocation ' s3a://dli-performance/tpcds/10240/catalog_page'")


    spark.sql("create external table catalog_returns\n(\n    " +
      "cr_returned_date_sk       bigint,\n    cr_returned_time_sk       " +
      "bigint,\n    cr_item_sk                bigint,\n    " +
      "cr_refunded_customer_sk   bigint,\n    cr_refunded_cdemo_sk      " +
      "bigint,\n    cr_refunded_hdemo_sk      bigint,\n    " +
      "cr_refunded_addr_sk       bigint,\n    cr_returning_customer_sk  " +
      "bigint,\n    cr_returning_cdemo_sk     bigint,\n    " +
      "cr_returning_hdemo_sk     bigint,\n    cr_returning_addr_sk      " +
      "bigint,\n    cr_call_center_sk         bigint,\n    cr_catalog_page_sk" +
      "        bigint,\n    cr_ship_mode_sk           bigint,\n    " +
      "cr_warehouse_sk           bigint,\n    cr_reason_sk              " +
      "bigint,\n    cr_order_number           bigint,\n    cr_return_quantity" +
      "        int,\n    cr_return_amount          double,\n    cr_return_tax" +
      "             double,\n    cr_return_amt_inc_tax     double,\n    " +
      "cr_fee                    double,\n    cr_return_ship_cost       " +
      "double,\n    cr_refunded_cash          double,\n    cr_reversed_charge" +
      "        double,\n    cr_store_credit           double,\n    " +
      "cr_net_loss               double\n)\nrow format delimited fields " +
      "terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/catalog_returns'")

    spark.sql("create external table catalog_sales\n(\n    cs_sold_date_sk   " +
      "        bigint,\n    cs_sold_time_sk           bigint,\n    " +
      "cs_ship_date_sk           bigint,\n    cs_bill_customer_sk       " +
      "bigint,\n    cs_bill_cdemo_sk          bigint,\n    cs_bill_hdemo_sk  " +
      "        bigint,\n    cs_bill_addr_sk           bigint,\n    " +
      "cs_ship_customer_sk       bigint,\n    cs_ship_cdemo_sk          " +
      "bigint,\n    cs_ship_hdemo_sk          bigint,\n    cs_ship_addr_sk   " +
      "        bigint,\n    cs_call_center_sk         bigint,\n    " +
      "cs_catalog_page_sk        bigint,\n    cs_ship_mode_sk           " +
      "bigint,\n    cs_warehouse_sk           bigint,\n    cs_item_sk        " +
      "        bigint,\n    cs_promo_sk               bigint,\n    " +
      "cs_order_number           bigint,\n    cs_quantity               int," +
      "\n    cs_wholesale_cost         double,\n    cs_list_price            " +
      " double,\n    cs_sales_price            double,\n    " +
      "cs_ext_discount_amt       double,\n    cs_ext_sales_price        " +
      "double,\n    cs_ext_wholesale_cost     double,\n    cs_ext_list_price " +
      "        double,\n    cs_ext_tax                double,\n    " +
      "cs_coupon_amt             double,\n    cs_ext_ship_cost          " +
      "double,\n    cs_net_paid               double,\n    " +
      "cs_net_paid_inc_tax       double,\n    cs_net_paid_inc_ship      " +
      "double,\n    cs_net_paid_inc_ship_tax  double,\n    cs_net_profit     " +
      "        double\n)\nrow format delimited fields terminated by '|' " +
      "\nlocation ' s3a://dli-performance/tpcds/10240/catalog_sales'")

    spark.sql("create external table customer_address\n(\n    ca_address_sk  " +
      "           bigint,\n    ca_address_id             string,\n    " +
      "ca_street_number          string,\n    ca_street_name            " +
      "string,\n    ca_street_type            string,\n    ca_suite_number   " +
      "        string,\n    ca_city                   string,\n    ca_county " +
      "                string,\n    ca_state                  string,\n    " +
      "ca_zip                    string,\n    ca_country                " +
      "string,\n    ca_gmt_offset             double,\n    ca_location_type  " +
      "        string\n)\nrow format delimited fields terminated by '|' " +
      "\nlocation ' s3a://dli-performance/tpcds/10240/customer_address'")

    spark.sql("create external table customer_demographics\n(\n    cd_demo_sk" +
      "                bigint,\n    cd_gender                 string,\n    " +
      "cd_marital_status         string,\n    cd_education_status       " +
      "string,\n    cd_purchase_estimate      int,\n    cd_credit_rating     " +
      "     string,\n    cd_dep_count              int,\n    " +
      "cd_dep_employed_count     int,\n    cd_dep_college_count      int \n)" +
      "\nrow format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/customer_demographics'")

    spark.sql("create external table customer\n(\n    c_customer_sk          " +
      "   bigint,\n    c_customer_id             string,\n    " +
      "c_current_cdemo_sk        bigint,\n    c_current_hdemo_sk        " +
      "bigint,\n    c_current_addr_sk         bigint,\n    " +
      "c_first_shipto_date_sk    bigint,\n    c_first_sales_date_sk     " +
      "bigint,\n    c_salutation              string,\n    c_first_name      " +
      "        string,\n    c_last_name               string,\n    " +
      "c_preferred_cust_flag     string,\n    c_birth_day               int," +
      "\n    c_birth_month             int,\n    c_birth_year              " +
      "int,\n    c_birth_country           string,\n    c_login              " +
      "     string,\n    c_email_address           string,\n    " +
      "c_last_review_date        string\n)\nrow format delimited fields " +
      "terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/customer'")

    spark.sql("create external table date_dim\n(\n    d_date_sk              " +
      "   bigint,\n    d_date_id                 string,\n    d_date         " +
      "           string,\n    d_month_seq               int,\n    d_week_seq" +
      "                int,\n    d_quarter_seq             int,\n    d_year  " +
      "                  int,\n    d_dow                     int,\n    d_moy " +
      "                    int,\n    d_dom                     int,\n    " +
      "d_qoy                     int,\n    d_fy_year                 int,\n  " +
      "  d_fy_quarter_seq          int,\n    d_fy_week_seq             int,\n" +
      "    d_day_name                string,\n    d_quarter_name            " +
      "string,\n    d_holiday                 string,\n    d_weekend         " +
      "        string,\n    d_following_holiday       string,\n    " +
      "d_first_dom               int,\n    d_last_dom                int,\n  " +
      "  d_same_day_ly             int,\n    d_same_day_lq             int,\n" +
      "    d_current_day             string,\n    d_current_week            " +
      "string,\n    d_current_month           string,\n    d_current_quarter " +
      "        string,\n    d_current_year            string \n)\nrow format " +
      "delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/date_dim'")

    spark.sql("create external table household_demographics\n(\n    " +
      "hd_demo_sk                bigint,\n    hd_income_band_sk         " +
      "bigint,\n    hd_buy_potential          string,\n    hd_dep_count      " +
      "        int,\n    hd_vehicle_count          int\n)\nrow format " +
      "delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/household_demographics'")

    spark.sql("create external table income_band(\n      ib_income_band_sk   " +
      "      bigint               \n,     ib_lower_bound            int      " +
      "                 \n,     ib_upper_bound            int\n)\nrow format " +
      "delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/income_band'")

    spark.sql("create external table inventory\n(\n    " +
      "inv_date_sk\t\t\tbigint,\n    inv_item_sk\t\t\tbigint,\n    " +
      "inv_warehouse_sk\t\tbigint,\n    inv_quantity_on_hand\tint\n)\nrow " +
      "format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/inventory'")

    spark.sql("create external table item\n(\n    i_item_sk                 " +
      "bigint,\n    i_item_id                 string,\n    i_rec_start_date  " +
      "        string,\n    i_rec_end_date            string,\n    " +
      "i_item_desc               string,\n    i_current_price           " +
      "double,\n    i_wholesale_cost          double,\n    i_brand_id        " +
      "        int,\n    i_brand                   string,\n    i_class_id   " +
      "             int,\n    i_class                   string,\n    " +
      "i_category_id             int,\n    i_category                string," +
      "\n    i_manufact_id             int,\n    i_manufact                " +
      "string,\n    i_size                    string,\n    i_formulation     " +
      "        string,\n    i_color                   string,\n    i_units   " +
      "                string,\n    i_container               string,\n    " +
      "i_manager_id              int,\n    i_product_name            " +
      "string\n)\nrow format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/item'")

    spark.sql("create external table promotion\n(\n    p_promo_sk            " +
      "    bigint,\n    p_promo_id                string,\n    " +
      "p_start_date_sk           bigint,\n    p_end_date_sk             " +
      "bigint,\n    p_item_sk                 bigint,\n    p_cost            " +
      "        double,\n    p_response_target         int,\n    p_promo_name " +
      "             string,\n    p_channel_dmail           string,\n    " +
      "p_channel_email           string,\n    p_channel_catalog         " +
      "string,\n    p_channel_tv              string,\n    p_channel_radio   " +
      "        string,\n    p_channel_press           string,\n    " +
      "p_channel_event           string,\n    p_channel_demo            " +
      "string,\n    p_channel_details         string,\n    p_purpose         " +
      "        string,\n    p_discount_active         string \n)\nrow format " +
      "delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/promotion'")

    spark.sql("create external table reason(\n      r_reason_sk              " +
      " bigint               \n,     r_reason_id               string        " +
      "      \n,     r_reason_desc             string                \n)\nrow" +
      " format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/reason'")

    spark.sql("create external table ship_mode(\n      sm_ship_mode_sk       " +
      "    bigint               \n,     sm_ship_mode_id           string     " +
      "         \n,     sm_type                   string                     " +
      " \n,     sm_code                   string                      \n,    " +
      " sm_carrier                string                      \n,     " +
      "sm_contract               string                      \n)\nrow format " +
      "delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/ship_mode'")

    spark.sql("create external table store_returns\n(\n    " +
      "sr_returned_date_sk       bigint,\n    sr_return_time_sk         " +
      "bigint,\n    sr_item_sk                bigint,\n    sr_customer_sk    " +
      "        bigint,\n    sr_cdemo_sk               bigint,\n    " +
      "sr_hdemo_sk               bigint,\n    sr_addr_sk                " +
      "bigint,\n    sr_store_sk               bigint,\n    sr_reason_sk      " +
      "        bigint,\n    sr_ticket_number          bigint,\n    " +
      "sr_return_quantity        int,\n    sr_return_amt             double," +
      "\n    sr_return_tax             double,\n    sr_return_amt_inc_tax    " +
      " double,\n    sr_fee                    double,\n    " +
      "sr_return_ship_cost       double,\n    sr_refunded_cash          " +
      "double,\n    sr_reversed_charge        double,\n    sr_store_credit   " +
      "        double,\n    sr_net_loss               double             \n)" +
      "\nrow format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/store_returns'")

    spark.sql("create external table store_sales\n(\n    ss_sold_date_sk     " +
      "      bigint,\n    ss_sold_time_sk           bigint,\n    ss_item_sk  " +
      "              bigint,\n    ss_customer_sk            bigint,\n    " +
      "ss_cdemo_sk               bigint,\n    ss_hdemo_sk               " +
      "bigint,\n    ss_addr_sk                bigint,\n    ss_store_sk       " +
      "        bigint,\n    ss_promo_sk               bigint,\n    " +
      "ss_ticket_number          bigint,\n    ss_quantity               int," +
      "\n    ss_wholesale_cost         double,\n    ss_list_price            " +
      " double,\n    ss_sales_price            double,\n    " +
      "ss_ext_discount_amt       double,\n    ss_ext_sales_price        " +
      "double,\n    ss_ext_wholesale_cost     double,\n    ss_ext_list_price " +
      "        double,\n    ss_ext_tax                double,\n    " +
      "ss_coupon_amt             double,\n    ss_net_paid               " +
      "double,\n    ss_net_paid_inc_tax       double,\n    ss_net_profit     " +
      "        double                  \n)\nrow format delimited fields " +
      "terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/store_sales'")

    spark.sql("create external table store\n(\n    s_store_sk                " +
      "bigint,\n    s_store_id                string,\n    s_rec_start_date  " +
      "        string,\n    s_rec_end_date            string,\n    " +
      "s_closed_date_sk          bigint,\n    s_store_name              " +
      "string,\n    s_number_employees        int,\n    s_floor_space        " +
      "     int,\n    s_hours                   string,\n    s_manager       " +
      "          string,\n    s_market_id               int,\n    " +
      "s_geography_class         string,\n    s_market_desc             " +
      "string,\n    s_market_manager          string,\n    s_division_id     " +
      "        int,\n    s_division_name           string,\n    s_company_id " +
      "             int,\n    s_company_name            string,\n    " +
      "s_street_number           string,\n    s_street_name             " +
      "string,\n    s_street_type             string,\n    s_suite_number    " +
      "        string,\n    s_city                    string,\n    s_county  " +
      "                string,\n    s_state                   string,\n    " +
      "s_zip                     string,\n    s_country                 " +
      "string,\n    s_gmt_offset              double,\n    s_tax_precentage  " +
      "        double                  \n)\nrow format delimited fields " +
      "terminated by '|' \nlocation ' s3a://dli-performance/tpcds/10240/store'")

    spark.sql("create external table time_dim\n(\n    t_time_sk              " +
      "   bigint,\n    t_time_id                 string,\n    t_time         " +
      "           int,\n    t_hour                    int,\n    t_minute     " +
      "             int,\n    t_second                  int,\n    t_am_pm    " +
      "               string,\n    t_shift                   string,\n    " +
      "t_sub_shift               string,\n    t_meal_time               " +
      "string\n)\nrow format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/time_dim'")

    spark.sql("create external table warehouse(\n      w_warehouse_sk        " +
      "    bigint               \n,     w_warehouse_id            string     " +
      "         \n,     w_warehouse_name          string                   " +
      "\n,     w_warehouse_sq_ft         int                       \n,     " +
      "w_street_number           string                      \n,     " +
      "w_street_name             string                   \n,     " +
      "w_street_type             string                      \n,     " +
      "w_suite_number            string                      \n,     w_city  " +
      "                  string                   \n,     w_county           " +
      "       string                   \n,     w_state                   " +
      "string                       \n,     w_zip                     string " +
      "                     \n,     w_country                 string         " +
      "          \n,     w_gmt_offset              double                  " +
      "\n)\nrow format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/warehouse'")

    spark.sql("create external table web_page(\n      wp_web_page_sk         " +
      "   bigint               \n,     wp_web_page_id            string      " +
      "        \n,     wp_rec_start_date        string                       " +
      "  \n,     wp_rec_end_date          string                         \n, " +
      "    wp_creation_date_sk       bigint                       \n,     " +
      "wp_access_date_sk         bigint                       \n,     " +
      "wp_autogen_flag           string                       \n,     " +
      "wp_customer_sk            bigint                       \n,     wp_url " +
      "                   string                  \n,     wp_type            " +
      "       string                      \n,     wp_char_count             " +
      "int                       \n,     wp_link_count             int       " +
      "                \n,     wp_image_count            int                 " +
      "      \n,     wp_max_ad_count           int\n)\nrow format delimited " +
      "fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/web_page'")

    spark.sql("create external table web_returns\n(\n    wr_returned_date_sk " +
      "      bigint,\n    wr_returned_time_sk       bigint,\n    wr_item_sk  " +
      "              bigint,\n    wr_refunded_customer_sk   bigint,\n    " +
      "wr_refunded_cdemo_sk      bigint,\n    wr_refunded_hdemo_sk      " +
      "bigint,\n    wr_refunded_addr_sk       bigint,\n    " +
      "wr_returning_customer_sk  bigint,\n    wr_returning_cdemo_sk     " +
      "bigint,\n    wr_returning_hdemo_sk     bigint,\n    " +
      "wr_returning_addr_sk      bigint,\n    wr_web_page_sk            " +
      "bigint,\n    wr_reason_sk              bigint,\n    wr_order_number   " +
      "        bigint,\n    wr_return_quantity        int,\n    wr_return_amt" +
      "             double,\n    wr_return_tax             double,\n    " +
      "wr_return_amt_inc_tax     double,\n    wr_fee                    " +
      "double,\n    wr_return_ship_cost       double,\n    wr_refunded_cash  " +
      "        double,\n    wr_reversed_charge        double,\n    " +
      "wr_account_credit         double,\n    wr_net_loss               " +
      "double\n)\nrow format delimited fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/web_returns'")

    spark.sql("create external table web_sales\n(\n    ws_sold_date_sk       " +
      "    bigint,\n    ws_sold_time_sk           bigint,\n    " +
      "ws_ship_date_sk           bigint,\n    ws_item_sk                " +
      "bigint,\n    ws_bill_customer_sk       bigint,\n    ws_bill_cdemo_sk  " +
      "        bigint,\n    ws_bill_hdemo_sk          bigint,\n    " +
      "ws_bill_addr_sk           bigint,\n    ws_ship_customer_sk       " +
      "bigint,\n    ws_ship_cdemo_sk          bigint,\n    ws_ship_hdemo_sk  " +
      "        bigint,\n    ws_ship_addr_sk           bigint,\n    " +
      "ws_web_page_sk            bigint,\n    ws_web_site_sk            " +
      "bigint,\n    ws_ship_mode_sk           bigint,\n    ws_warehouse_sk   " +
      "        bigint,\n    ws_promo_sk               bigint,\n    " +
      "ws_order_number           bigint,\n    ws_quantity               int," +
      "\n    ws_wholesale_cost         double,\n    ws_list_price            " +
      " double,\n    ws_sales_price            double,\n    " +
      "ws_ext_discount_amt       double,\n    ws_ext_sales_price        " +
      "double,\n    ws_ext_wholesale_cost     double,\n    ws_ext_list_price " +
      "        double,\n    ws_ext_tax                double,\n    " +
      "ws_coupon_amt             double,\n    ws_ext_ship_cost          " +
      "double,\n    ws_net_paid               double,\n    " +
      "ws_net_paid_inc_tax       double,\n    ws_net_paid_inc_ship      " +
      "double,\n    ws_net_paid_inc_ship_tax  double,\n    ws_net_profit     " +
      "        double\n)\nrow format delimited fields terminated by '|' " +
      "\nlocation ' s3a://dli-performance/tpcds/10240/web_sales'")

    spark.sql("create external table web_site\n(\n    web_site_sk           " +
      "bigint,\n    web_site_id           string,\n    web_rec_start_date    " +
      "string,\n    web_rec_end_date      string,\n    web_name              " +
      "string,\n    web_open_date_sk      bigint,\n    web_close_date_sk     " +
      "bigint,\n    web_class             string,\n    web_manager           " +
      "string,\n    web_mkt_id            int,\n    web_mkt_class         " +
      "string,\n    web_mkt_desc          string,\n    web_market_manager    " +
      "string,\n    web_company_id        int,\n    web_company_name      " +
      "string,\n    web_street_number     string,\n    web_street_name       " +
      "string,\n    web_street_type       string,\n    web_suite_number      " +
      "string,\n    web_city              string,\n    web_county            " +
      "string,\n    web_state             string,\n    web_zip               " +
      "string,\n    web_country           string,\n    web_gmt_offset        " +
      "double,\n    web_tax_percentage    double\n)\nrow format delimited " +
      "fields terminated by '|' \nlocation ' " +
      "s3a://dli-performance/tpcds/10240/web_site'")

    println("######################Successfully created ext tables")
    spark.stop()

    println("Successfully created external table#####################")
    spark.stop()
  }
}
// scalastyle:on println
