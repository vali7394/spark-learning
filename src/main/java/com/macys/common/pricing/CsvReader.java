package com.macys.common.pricing;

/*
  @created 7/2/21
  @Author b008245 - Meeravali Shaik 
 */

import java.time.LocalDate;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_timestamp;
public class CsvReader {

    public static void main(String[] args) {
      /*  SparkSession session = SparkSession.builder().master("local[*]").appName("CSV-READER")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();*/
       SparkSession session = SparkSession.builder().appName("CSV-READER")
            .getOrCreate();
        Configuration configuration = session.sessionState().newHadoopConf();
        String storagePath = configuration.get("gcs.bucket.path","gs://prod-sku-striim-data/");
        String prodSkuFilePath = configuration.get("prodsku.fileName", "prod-sku-test.csv");
        String spannerFilePth = configuration.get("spanner.fileName", "spanner-prod-test.csv");
        String dateFilter = configuration.get("date.filter","2021-05-10 00:00:11");
        Dataset<Row> onPremProdSku = session.read()
            .option("mode", "DROPMALFORMED")
            .option("header", "true")
            .csv(storagePath+prodSkuFilePath);
        Dataset<Row> spannerProdSku = session.read()
            .option("mode", "DROPMALFORMED")
            .option("header", "true")
            .csv(storagePath+spannerFilePth);
        System.out.println("Spanner Skus" + spannerProdSku.count());
        Dataset<Row> rowsMissingInSpanner = onPremProdSku.join(spannerProdSku,onPremProdSku.col("SKU_UPC_NBR").equalTo(spannerProdSku.col("SkuUpcNbr")),"left_anti")
            .where(onPremProdSku.col("LAST_UPD_TS").cast("timestamp").lt(to_timestamp(lit(dateFilter))));
        /* Dataset<Row> rowsMissingInSpanner = onPremProdSku.join(spannerProdSku,onPremProdSku.col("SkuUpcNbr").equalTo(spannerProdSku.col("SkuUpcNbr")))
            .where(spannerProdSku.col("SkuUpcNbr").isNull())
            .select(onPremProdSku.col("SkuUpcNbr"),onPremProdSku.col("LastUpdTs"));*/
        rowsMissingInSpanner.write()
            .format("csv")
            .mode(SaveMode.Overwrite)
            .save(storagePath+ "result-"+ LocalDate.now());
    }


    public static Configuration getConfiguration(final SparkSession session) {
        return session.sessionState().newHadoopConf();
    }

}
