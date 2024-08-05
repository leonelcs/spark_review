package com.jmb;

import com.jmb.mappers.CarMapper;
import com.jmb.model.Car;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetPOJOs {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetPOJOs.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    public static final String PATH_RESOURCES = "src/main/resources/spark-data/cars.csv";

    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        DatasetPOJOs app = new DatasetPOJOs();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    public void init() throws Exception {

        //Create the Spark Session
        SparkSession session = SparkSession.builder()
                .appName("DatasetPOJOs")
                .master("local").getOrCreate();

        //Ingest data from CSV file into a Dataset of POJOs
        Dataset<Row> ds = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        //Show the Dataset
        ds.show();
        ds.printSchema();

        Dataset<Car> carDataset = ds.map(new CarMapper(), Encoders.bean(Car.class));

        //Show the Dataset of POJOs
        //Print the dataset schema and show the first 5 rows
        carDataset.printSchema();
        carDataset.show(5);

        //Stop the Spark Session
        session.stop();
    }
}
