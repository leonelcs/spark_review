package com.jmb;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;

public class DataPartitioning {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTransformationsPartTwo.class);
    private static final String SPARK_FILE_FORMAT = "csv";
    private static final String PATH_RESOURCES = "src/main/resources/spark-data/sales_information.csv";

    public static void main(String[] args) {
        LOGGER.info("Application starting up");
        DataPartitioning app = new DataPartitioning();
        app.run();
        LOGGER.info("Application gracefully exiting...");
    }

    private void run() {
        LOGGER.info("Creating a DataFrame from a CSV file");
        // Create a Spark session
        SparkSession session = SparkSession.builder()
                .appName("DataPartitioning")
                .master("local").getOrCreate();

        // Create a DataFrame from a CSV file
        Dataset<Row> df = session.read()
                .format(SPARK_FILE_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        Dataset<Row> distDf = df.repartition(4);

        System.out.println("Number of partitions: " + distDf.rdd().partitions().length);

        distDf.show(5);

        ForeachPartitionFunction<Row> printPartition = (iterator) -> {
            System.out.println("PARTITION CONTENTS: ");
            while (iterator.hasNext()) {
                System.out.println("Row Value: " + iterator.next().toString());
            }
        };

//        distDf.foreachPartition(printPartition);

        //Sort the sales per seller ID
        Dataset<Row> sortedSales = distDf.sort(col("id"));

        //Print the results per partition.
        sortedSales.foreachPartition(printPartition);
    }
}
