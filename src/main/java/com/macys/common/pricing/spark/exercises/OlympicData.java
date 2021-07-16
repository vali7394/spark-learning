package com.macys.common.pricing.spark.exercises;/*
  @created 7/16/21
  @Author b008245 - Meeravali Shaik 
 */

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/*
Find the total number of medals won by each country in swimming.
b) Find the number of medals that India won year wise.
c) Find the total number of medals won by each country.
 */

public class OlympicData {

    public static void main(String[] args) {
        SparkSession session = SparkUtil.getSparkSession();
        StructType structType = DataTypes.createStructType(new StructField[]{
            createStructField("PersonName",StringType,false),
            createStructField("Age",IntegerType,false),
            createStructField("Country",StringType,false),
            createStructField("EventYear",StringType,false),
            createStructField("EventCloseDate",DateType,false),
            createStructField("Sport",StringType,false),
            createStructField("Gold",IntegerType,false),
            createStructField("Silver",IntegerType,false),
            createStructField("Bronze",IntegerType,false),
            createStructField("Total",IntegerType,false)

        });
        Dataset<Row> olympicData = session.read()
            .schema(structType)
            .option("dateFormat", "dd-mm-yyyy")
            .csv("/Users/b008245/Desktop/digital-repos/searcher-indexer/csv-spark-example/src/main/resources/olympic_data.csv");
        olympicData.groupBy(col("Country"),col("Sport"))
            .agg(sum(col("Total")).as("TotalMedals"))
            .where(col("Sport").equalTo("Swimming"))
            .orderBy(col("TotalMedals").desc()).explain();

        olympicData.filter(col("Sport").equalTo("Swimming"))
            .groupBy(col("Country"),col("Sport"))
            .agg(sum(col("Total")).as("TotalMedals"))
            .orderBy(col("TotalMedals").desc()).show();

        olympicData.filter(col("Country").equalTo("India")).groupBy(col("EventYear")).agg(sum(col("Total")).as("TotalWon"))
        .orderBy(col("TotalWon").desc()).show();

        olympicData.groupBy(col("Country"))
            .agg(sum(col("Total")).as("TotalWon"))
            .orderBy(col("TotalWon").desc()).show();
    }

}
