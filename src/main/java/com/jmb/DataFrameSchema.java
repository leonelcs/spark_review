package com.jmb;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static java.util.stream.Collectors.*;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class DataFrameSchema {

    private static final Logger logger = LoggerFactory.getLogger(DataFrameSchema.class);
    private static final String SPARK_FILE_FORMAT = "csv";
    private static final String PATH_RESOURCES = "src/main/resources/spark-data/classic_books.csv";



    public static void main(String[] args) {
        logger.info("Application starting up");
        DataFrameSchema app = new DataFrameSchema();
        app.run();
        logger.info("Application gracefully exiting...");
    }

    private void run() {
        logger.info("Creating a DataFrame from a CSV file");
        // Create a Spark session
        SparkSession session = SparkSession.builder()
                .appName("DataFrameSchema")
                .master("local").getOrCreate();

        // Create a DataFrame from a CSV file
        Dataset<Row> df = session.read()
                .format(SPARK_FILE_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        // Print the schema without non-bibliography columns
        Dataset<Row> dfFewerColumns = keepOnlyBibliography(df);

        Dataset<Row> renamedDf = renameColumns(dfFewerColumns);

        //create a list of distinct subjects
        Dataset<Row> subjectsUniqueDf = createSubject(renamedDf).distinct();

        //create a list of books
        Dataset<Row> booksDf = createBooksDf(renamedDf);

        logger.info("Schema of Books DF as JSON: " + booksDf.schema().prettyJson());

    }

    private Dataset<Row> createSubject(Dataset<Row> df) {
        return df.withColumn("SubjectID", concat(df.col("subjects"), lit("_S")))
                .drop(Arrays.stream(df.schema().names()).filter(name -> !name.startsWith("subjects")).toArray(name -> new String[name]));
    }

    private Dataset<Row> createBooksDf(Dataset<Row> renamedDf) {
        return renamedDf
                .withColumn("BookID",
                        concat(renamedDf.col("title"), lit("_"), renamedDf.col("author_name")))
                .withColumn("SubjectID", concat(renamedDf.col("subjects"), lit("_S")))
                .drop("subjects");
    }

    private Dataset<Row> renameColumns(Dataset<Row> df) {
        return df.withColumnRenamed("bibliography.congress classifications", "classifications")
                .withColumnRenamed("bibliography.languages", "languages")
                .withColumnRenamed("bibliography.subjects", "subjects")
                .withColumnRenamed("bibliography.title","title")
                .withColumnRenamed("bibliography.type","type")
                .withColumnRenamed("bibliography.author.birth", "author_birth")
                .withColumnRenamed("bibliography.author.death", "author_death")
                .withColumnRenamed("bibliography.author.name", "author_name")
                .withColumnRenamed("bibliography.publication.day", "publication_day")
                .withColumnRenamed("bibliography.publication.full", "publication_full")
                .withColumnRenamed("bibliography.publication.month","publication_month")
                .withColumnRenamed("bibliography.publication.month name", "publication_month_name")
                .withColumnRenamed("bibliography.publication.year", "publication_year");
    }

    private Dataset<Row> keepOnlyBibliography(Dataset<Row> df) {
        Arrays.stream(df.schema().names()).collect(groupingBy(name -> name.startsWith("bibliography")));
        String[] names = Arrays.stream(df.schema().names()).filter(name -> !name.startsWith("bibliography")).toArray(name -> new String[name]);
        return df.drop(names);
    }

}
