package com.macys.common.pricing.spark.exercises;
/*
  @created 7/14/21
  @Author b008245 - Meeravali Shaik 
 */

/*
Create a new column called "hashed_bill" defined as follows:
- if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text.
    E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
- if the order_id is odd: apply SHA256 hashing to the bill text
Finally, check if there are any duplicate on the new column
 */

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.util.Arrays;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class SparkExerciseFour {

    public static void main(String[] args) {
        SparkSession session = SparkUtil.getSparkSession();
        session.udf().register("hash_udf",hashUdf, DataTypes.StringType);
        Dataset<Row> orderData = SparkUtil.getOrderData(session);

        Dataset<Row> hashedOrders = orderData.withColumn("hashed_bill",
            callUDF("hash_udf", col("bill_raw_text"),
                col("order_id").cast(IntegerType).mod(2).equalTo(0)));
        hashedOrders.groupBy(col("hashed_bill"))
            .agg(count("*").alias("bill_count"))
            .where(col("bill_count").gt(1))
            .show();
    }

    private static final UDF2 hashUdf = new UDF2<String,Boolean,String>(){
        @Override
        public String call(String colValue, Boolean isOdd) {
            return applyHashing(colValue,isOdd);
        }

        final  String hash = "35454B055CC325EA1AF2126E27707052";

        private String applyHashing(String colValue, boolean isOdd){
            if(isOdd){
                return applySh256Hashing(colValue);
            } else {
                long count = Arrays.stream(colValue.split(""))
                    .filter("A"::equals)
                    .count();
                String hashedValue = colValue;
                for(int i=0 ; i<count ; i++){
                    hashedValue = applyMd5Hashing(hashedValue);
                }
                return hashedValue;
            }
        }

        private String applyMd5Hashing(String colValue){
            return DigestUtils
                .md5Hex(colValue).toUpperCase();
        }

        private String applySh256Hashing(String colValue){
            return DigestUtils
                .sha256Hex(colValue).toUpperCase();
        }
    };


}
