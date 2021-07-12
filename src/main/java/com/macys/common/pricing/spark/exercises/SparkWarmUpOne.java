package com.macys.common.pricing.spark.exercises;

/*
  @created 7/10/21
  @Author - Meeravali Shaik
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

@Slf4j
public class SparkWarmUpOne {

    /*

    1) Find out how many orders, how many products and how many sellers are in the data.
    2) How many products have been sold at least once?
    3) Which is the product contained in more orders?

     */

    public static void main(String[] args) {
        SparkSession session = SparkUtil.getSparkSession();
        Dataset<Row> productData = SparkUtil.getProductData(session);
        Dataset<Row> orderData = SparkUtil.getOrderData(session);
        Dataset<Row> sellersData = SparkUtil.getSellerData(session);
        getCount(productData,"Product");
        getCount(orderData,"Orders");
        getCount(sellersData,"Sellers");
        getDistinctProductCount(orderData);
        getTopSellingProduct(orderData);
    }



    private static void getCount(Dataset<Row> dataset, String dataSetName){
        log.info("Total no of rows in the {} is {}",dataSetName,dataset.count());
        dataset.show();
    }

    private static void getDistinctProductCount(Dataset<Row> dataset){
        dataset.agg(countDistinct(col("product_id")).as("Distinct Products Sold"))
            .show();
    }

    private static void getTopSellingProduct(Dataset<Row> dataset) {
        dataset.groupBy(col("product_id"))
            .agg(count("*").as("Product_Count"))
            .orderBy(col("Product_Count").desc())
            .limit(1)
            .show();
    }

}
