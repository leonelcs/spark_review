package com.jmb;

import com.jmb.reducers.FarmStandSalesReducer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.Seq;

import static org.apache.spark.sql.functions.*;

public class BasicActionsPartTwo {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTransformationsPartTwo.class);
    private static final String SPARK_FILE_FORMAT = "csv";
    private static final String PATH_RESOURCES = "src/main/resources/spark-data/farm_stands_sales_austin.csv";

    public static void main(String[] args) {
        LOGGER.info("Application starting up");
        BasicActionsPartTwo app = new BasicActionsPartTwo();
        app.run();
        LOGGER.info("Application gracefully exiting...");
    }

    private void run() {
        LOGGER.info("Creating a DataFrame from a CSV file");
        // Create a Spark session
        SparkSession session = SparkSession.builder()
                .appName("BasicActionsPartTwo")
                .master("local").getOrCreate();

        // Create a DataFrame from a CSV file
        Dataset<Row> df = session.read()
                .format(SPARK_FILE_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        df.show(5);

        //Drop rows not used in calculation
        Dataset<Row> numericFieldsDf = df.drop("farm_stand","month","year","days","visitors");

        Row returnRow = numericFieldsDf.reduce(new FarmStandSalesReducer());

        System.out.println("Total Calculated Sales: " + returnRow.prettyJson());

        df.agg(max(df.col("total_sales"))).show();

        df.agg(avg(df.col("total_snap_sales"))).show();

        df.agg(min(df.col("total_snap_sales"))).show();

        df.agg(mean(df.col("total_double_sales"))).show();


    }
}
