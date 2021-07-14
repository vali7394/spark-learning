package com.macys.common.pricing.spark.exercises;
/*
  @created 7/13/21
  @Author b008245 - Meeravali Shaik 
 */

import static org.apache.spark.sql.functions.aggregate;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dense_rank;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

/*

“Who are the second most selling and the least selling persons (sellers) for each product? Who are those for the product with product_id = 0”.

    Who are the second most selling and the least selling persons (sellers) for each product

 */

public class SparkExerciseThree {

    public static void main(String[] args) {
        SparkSession session = SparkUtil.getSparkSession();
        Dataset<Row> orderData = SparkUtil.getOrderData(session);
        Dataset<Row> sellerData = SparkUtil.getSellerData(session);
        Dataset<Row> orders_table = orderData.groupBy(col("product_id"),col("seller_id"))
            .agg(sum(col("num_pieces_sold").cast(DoubleType)).as("total_products_by_Seller"));
        WindowSpec windowDesc = Window.partitionBy(col("product_id")).orderBy(col("total_products_by_Seller").desc());
        WindowSpec windowAesc = Window.partitionBy(col("product_id")).orderBy(col("total_products_by_Seller").asc());

        Dataset<Row> orders_with_rank = orders_table.withColumn("rank_desc",dense_rank().over(windowDesc))
            .withColumn("rank_asc",dense_rank().over(windowAesc));
        Dataset<Row> single_seller = orders_with_rank.where(col("rank_desc").equalTo(col("rank_asc")))
            .select(col("product_id").alias("Single_Seller_product"),col("seller_id").alias("Single_Product_Seller")
                ,lit("Only seller or multiple sellers with the same results").alias("type"));
        Dataset<Row> second_seller = orders_with_rank.where(col("rank_desc").equalTo(lit(2)))
            .select(col("product_id").alias("Second_Seller_product"),col("seller_id").alias("Second_Product_Seller")
                ,lit("Second top seller").alias("type"));
        Dataset<Row> least_seller = orders_with_rank.where(col("rank_asc").equalTo(lit(1)))
            .select(col("product_id"),col("seller_id")
                ,lit("Least Seller").alias("type"));

        Dataset<Row> least_seller_without_dups = least_seller.join(single_seller,
            single_seller.col("Single_Product_Seller").equalTo(least_seller.col("seller_id"))
                .and(single_seller.col("Single_Seller_product")
                    .equalTo(least_seller.col("product_id"))), "left_anti")
            .join(second_seller,
                second_seller.col("Second_Product_Seller").equalTo(least_seller.col("seller_id"))
                    .and(second_seller.col("Second_Seller_product")
                        .equalTo(least_seller.col("product_id"))), "left_anti");
        Dataset<Row> unionData = least_seller_without_dups.select(col("product_id"),col("seller_id"),col("type"))
            .union(single_seller.select(col("Single_Seller_product"),col("Single_Product_Seller"),col("type")))
            .union(second_seller.select(col("Second_Seller_product"),col("Second_Product_Seller"),col("type")));
        unionData.where(col("product_id").equalTo(0)).show();
    }

}
