package com.jmb;

import com.jmb.mappers.AirportsMapGrouper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class DataGrouping {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataGrouping.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    private static final String PATH_RESOURCES = "src/main/resources/spark-data/airlines.csv";

    public static void main(String[] args) {
        LOGGER.info("Application starting up");
        DataGrouping app = new DataGrouping();
        app.run();
        LOGGER.info("Application gracefully exiting...");
    }

    private void run() {
        LOGGER.info("Creating a DataFrame from a CSV file");
        // Create a Spark session
        SparkSession session = SparkSession.builder()
                .appName("DataGrouping")
                .master("local").getOrCreate();

        // Create a DataFrame from a CSV file
        Dataset<Row> df = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        df.show(5);

        Dataset<Row> groupedDf = df.groupBy(df.col("`Airport.Code`"), df.col("`Time.Month`"))
                .agg(functions.sum("`Statistics.Flights.Total`"));

        groupedDf.show(5);

        //Filter out all flights except for the year 2005
        Dataset<Row> filteredDf = df.filter(df.col("`Time.Label`").contains("2005"));
        filteredDf.show(10);

        int airportCodeRowIndex = 0;
        KeyValueGroupedDataset keyValueGroupedDataset =
                filteredDf.groupByKey((MapFunction<Row, String>) row -> row.getString(airportCodeRowIndex), Encoders.STRING());

        Dataset<Row> summarisedResults = keyValueGroupedDataset.mapGroups(new AirportsMapGrouper(),
                Encoders.row(new StructType(AirportsMapGrouper.defineRowSchema())));

        summarisedResults.show(5);

    }
}
