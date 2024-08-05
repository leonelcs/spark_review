package com.jmb;

import com.jmb.filters.HerbsFilter;
import com.jmb.mappers.EmptyToUnderscoreMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class BasicTransformations {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTransformations.class);
    private static final String SPARK_FILE_FORMAT = "csv";
    private static final String PATH_RESOURCES = "src/main/resources/spark-data/generic-food.csv";

    public static void main(String[] args) {
        LOGGER.info("Application starting up");
        BasicTransformations app = new BasicTransformations();
        app.run();
        LOGGER.info("Application gracefully exiting...");
    }

    public static void run() {
            LOGGER.info("Creating a DataFrame from a CSV file");
        // Create a Spark session
        SparkSession session = SparkSession.builder()
                .appName("BasicTransformations")
                .master("local").getOrCreate();

        // Create a DataFrame from a CSV file
        Dataset<Row> df = session.read()
                .format(SPARK_FILE_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        // Print the first 5 rows
        df.show(5);

        Dataset<Row> transformedDf = applyMapFunction(df);

        transformedDf.show(5);

        Dataset<Row> filteredDf = applyFilterFunction(transformedDf);

        filteredDf.show(5);

        Dataset<Row> inlineFiltered = transformedDf.filter(transformedDf.col("GROUP").equalTo("Vegetables"));

        inlineFiltered.show(5);

        Row[] result = (Row[]) inlineFiltered.take(4);

        LOGGER.info("=============== Printing Rows =============== ");
        Arrays.asList(result).stream().forEach(row -> LOGGER.info("Row: " + row.mkString(",")));
        LOGGER.info("=============== End Printing Rows =============== ");

        Row[] collected = (Row[]) inlineFiltered.collect();

        //Display the size of the Java list with the collected rows
        LOGGER.info("Total Collected Rows for DataFrame: " + collected.length);

    }

    private static Dataset<Row> applyMapFunction(Dataset<Row> inputDf) {
        //following spark 3.5.1 API
        return inputDf.map(new EmptyToUnderscoreMapper("GROUP"), Encoders.row(inputDf.schema()));
    }

    private static Dataset<Row> applyFilterFunction(Dataset<Row> inputDf) {
        //following spark 3.5.1 API
        return inputDf.filter(new HerbsFilter());
    }


}
