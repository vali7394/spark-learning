package com.macys.common.pricing.spark.exercises;

/*
  @created 7/10/21
  @Author - Meeravali Shaik
 */

/*
https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkUtil {

    public static SparkSession getSparkSession(){
        return SparkSession.builder()
            .master("local[*]")
            .appName("Spark-Order-Processing")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.autoBroadcastJoinThreshold", -1)
            //.config("spark.executor.memory", "500mb")
            .getOrCreate();
    }

    public static Dataset<Row> getDataFromFile(SparkSession session, String filePath){
        return session.read()
            .parquet(filePath);
    }

    public static Dataset<Row> getProductData(SparkSession session){
        return SparkUtil.getDataFromFile(session,"/Users/b008245/Documents/Spark-learning/DatasetToCompleteTheSixSparkExercises/products_parquet/");
    }

    public static Dataset<Row> getSellerData(SparkSession session){
        return SparkUtil.getDataFromFile(session,"/Users/b008245/Documents/Spark-learning/DatasetToCompleteTheSixSparkExercises/sellers_parquet/");
    }

    public static Dataset<Row> getOrderData(SparkSession session){
        return SparkUtil.getDataFromFile(session,"/Users/b008245/Documents/Spark-learning/DatasetToCompleteTheSixSparkExercises/sales_parquet/");
    }

}
