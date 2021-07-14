package com.macys.common.pricing.spark.exercises;
/*
  @created 7/10/21
  @Author b008245 - Meeravali Shaik 
 */

/*
    What is the average revenue of the orders?
    https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c
    https://0x0fff.com/spark-memory-management/
    https://coxautomotivedatasolutions.github.io/datadriven/spark/data%20skew/joins/data_skew/
 */

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class SparkExerciseOne {

    public static void main(String[] args) {
        SparkSession session = SparkUtil.getSparkSession();
        Dataset<Row> orderData = SparkUtil.getOrderData(session);
        Dataset<Row> productData = SparkUtil.getProductData(session);


        /*
           My Approach -
           Downsides : dataSkew on ProductId 0.

          orderData.join(productData,orderData.col("product_id").equalTo(productData.col("product_id")))
            .agg(round(avg(col("num_pieces_sold").multiply(col("price"))),2).as("avg_order_value")).show();

        /*
            Key salting approach to resolve dataSkew.
         */

      //  orderData.printSchema();
        Row[] highVolProducts = (Row[]) orderData.groupBy(col("product_id")).agg(count("*").as("Product_count"))
            .sort(col("Product_count").desc())
            .limit(10)
            .collect();

        orderData.cache();

        List<String> replicated_products = new ArrayList<>();
        List<Row> l = new ArrayList<>();

        StructType schemata = DataTypes.createStructType(
            new StructField[]{
                createStructField("product_id", StringType, false),
                createStructField("replication", IntegerType, false)
            });

        for(Row product : highVolProducts){
            replicated_products.add(product.getString(0));
            for(int i=1 ; i<101; i++){
                l.add(RowFactory.create(product.getString(0),i));
            }
        }

        Dataset<Row> replicatedDataSet = session.createDataFrame(l,schemata);

        Dataset<Row> productsTable = productData.join(broadcast(replicatedDataSet),productData.col("product_id").equalTo(replicatedDataSet.col("product_id")),"left")
            .withColumn("salted_join_key", when(replicatedDataSet.col("replication").isNull(),productData.col("product_id"))
            .otherwise(concat(replicatedDataSet.col("product_id"),lit("-"),replicatedDataSet.col("replication"))));

        Dataset<Row> ordersTable = orderData.withColumn("salted_join_key",
            when(orderData.col("product_id").isin(replicated_products.stream().toArray(String[]::new)),concat(orderData.col("product_id"),lit("-"),lit(Math.round(Math.random()*100)).cast(IntegerType)))
            .otherwise(orderData.col("product_id"))
        );

        ordersTable.join(productsTable,ordersTable.col("salted_join_key").equalTo(productsTable.col("salted_join_key")))
            .agg(round(avg(col("num_pieces_sold").multiply(col("price"))),2).as("avg_order_value")).show();

        System.out.println("ok");

    }

}
