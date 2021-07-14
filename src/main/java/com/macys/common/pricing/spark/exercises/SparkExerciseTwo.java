package com.macys.common.pricing.spark.exercises;

/*
  @created 7/12/21
  @Author b008245 - Meeravali Shaik 
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.grouping;
import static org.apache.spark.sql.functions.sum;

/*
  For each seller, what is the average % contribution of an order to the seller's daily quota?

    # Example
    If Seller_0 with `quota=250` has 3 orders:
    Order 1: 10 products sold
    Order 2: 8 products sold
    Order 3: 7 products sold
    The average % contribution of orders to the seller's quota would be:
    Order 1: 10/250 = 0.04
    Order 2: 8/250 = 0.032
    Order 3: 7/250 = 0.028
    Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333
===========

 */
public class SparkExerciseTwo {

    public static void main(String[] args) {
        SparkSession session = SparkUtil.getSparkSession();
        Dataset<Row> orderData = SparkUtil.getOrderData(session);
        Dataset<Row> sellerData = SparkUtil.getSellerData(session);
        orderData.show();
        sellerData.show();
      /*  Dataset<Row> orderDataByDay = orderData.groupBy(col("date"),col("seller_id"))
            .agg(sum(col("num_pieces_sold")).as("Total_Sold_in_Day"),count(col("order_id")).as("Seller_order_Count_By_Day"));
        orderDataByDay.join(broadcast(sellerData),orderDataByDay.col("seller_id").equalTo(sellerData.col("seller_id")))
        .agg(avg((col("Total_Sold_in_Day").divide(col("daily_target"))))).show();
        ;*/
      /*  orderData.join(broadcast(sellerData),orderData.col("seller_id").equalTo(sellerData.col("seller_id")))
            .groupBy(orderData.col("date").and(orderData.col("seller_id"))).agg(count("num_pieces_sold"))*/

        /*
           Use broadcast join since the sellerData is very small & can easily fit in memory.
                broadcast(sellerData)
         */

        orderData.join(broadcast(sellerData),orderData.col("seller_id").equalTo(sellerData.col("seller_id")))
            .withColumn("ratio",col("num_pieces_sold").divide(col("daily_target")))
            .groupBy(orderData.col("seller_id")).agg(avg(col("ratio"))).show();

    }

}
