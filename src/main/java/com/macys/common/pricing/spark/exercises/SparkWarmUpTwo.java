package com.macys.common.pricing.spark.exercises;
/*
  @created 7/10/21
  @Author  - Meeravali Shaik
 */

/*
    1) How many distinct products have been sold in each day?

 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class SparkWarmUpTwo {

    public static void main(String[] args) {
        SparkSession session = SparkUtil.getSparkSession();
        Dataset<Row> orderData = SparkUtil.getOrderData(session);
       // orderData.show(5);
        orderData.groupBy(col("date"))
            .agg(countDistinct(col("product_id")).as("Uniq_product_sold_day"))
            .show();
    }

}
