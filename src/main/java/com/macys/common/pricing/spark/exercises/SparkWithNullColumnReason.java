package com.macys.common.pricing.spark.exercises;

/*
  @created 7/8/21
  @Author b008245 - Meeravali Shaik 
 */

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.when;

public class SparkWithNullColumnReason {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[*]").appName("NULL_COLUMN_PROCESSOR")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();
        StructType schemata = DataTypes.createStructType(
            new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("name", StringType, false),
                createStructField("qty", IntegerType, true),
                createStructField("vendor_id", StringType, true),
            });
        Dataset<Row> fruits = session.createDataFrame(getRowList(),schemata);
        Dataset<Row> fruitsWithDetails = fruits.filter(fruits.col("qty").isNotNull().and(fruits.col("vendor_id").isNotNull()));
        Dataset<Row> fruitsWithoutDetails = fruits.filter(fruits.col("qty").isNull().or(fruits.col("vendor_id").isNull()))
            .withColumn("reason",when(fruits.col("qty").isNull().and(fruits.col("vendor_id").isNull()),"Qty and Vendor id is null")
            .when(fruits.col("qty").isNull(),"Qty is null")
            .otherwise("Vemdor id is null"));

        fruitsWithDetails.show(false);
        fruitsWithoutDetails.select("id","name","reason").show(false);
    }


    private static List<Row> getRowList(){
        Row r1 = RowFactory.create(1,"Mangoes", 66, "v432");
        Row r2 = RowFactory.create(2,"Mangoes", 66, null);
        Row r3 = RowFactory.create(3,"apples", null, "v4312");
        Row r4 = RowFactory.create(4,"apples", 66, "v432");
        Row r6 = RowFactory.create(6,"strawberries", null, null);
        Row r7 = RowFactory.create(7,"strawberries", 66, null);
        Row r8 = RowFactory.create(8,"cherries", null, null);
        Row r9= RowFactory.create(9,"cherries", null, "v432");
        Row r10 = RowFactory.create(10,"grapes", 66, null);
        Row r11 = RowFactory.create(11,"grapes", 66, "v432");
        Row r12 = RowFactory.create(12,"Mangoes", 66, "v432");
        Row r13 = RowFactory.create(13,"apples", 66, "v432");
        Row r14 = RowFactory.create(14,"apples", null, "v4312");
        Row r5 = RowFactory.create(5,"Mangoes", 66, "v4322");
        return ImmutableList.of(r1, r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13,r14);
    }
}
