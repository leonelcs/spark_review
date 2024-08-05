package com.jmb;

import com.jmb.mappers.StudentGradesFlatMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class BasicTransformationsPartTwo {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTransformationsPartTwo.class);
    private static final String SPARK_FILE_FORMAT = "csv";
    private static final String PATH_RESOURCES = "src/main/resources/spark-data/students_subjects.csv";

    public static void main(String[] args) {
        LOGGER.info("Application starting up");
        BasicTransformationsPartTwo app = new BasicTransformationsPartTwo();
        app.run();
        LOGGER.info("Application gracefully exiting...");
    }

    public static void run() {
            LOGGER.info("Creating a DataFrame from a CSV file");
        // Create a Spark session
        SparkSession session = SparkSession.builder()
                .appName("BasicTransformationsPartTwo")
                .master("local").getOrCreate();

        // Create a DataFrame from a CSV file
        Dataset<Row> df = session.read()
                .format(SPARK_FILE_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        df.show();

        LOGGER.info("======== Input any non blank key and tap Enter to see results of GRADE column split... ==========");

        Dataset<Row> renamedDf = df.withColumnRenamed("Student_Name", "Student Name");

        Dataset<Row> studentsDf = renamedDf.select(renamedDf.col("Student Name"),
                                        split(renamedDf.col("Grades"), " "));
        studentsDf.show();

        //Rename second column to something more meaningful
        LOGGER.info("======== Input any non blank key and tap Enter to see results renaming column 'split' ... ==========");

        //Rename second column to something more meaningful
        Dataset<Row> renamedStudentsDf = studentsDf.withColumnRenamed("split(Grades,  , -1)", "Grades");
        renamedStudentsDf.show();

        //Rename second column to something more meaningful
        LOGGER.info("======== Flatted version ... ==========");

        Dataset<Row> flattenedDf = renamedStudentsDf.flatMap(new StudentGradesFlatMapper(), Encoders.row(getFlattedSchema()));
        flattenedDf.show();

    }

    private static StructType getFlattedSchema() {
        StructField[] fields = new StructField[]{
                new StructField("Name", DataTypes.StringType, true, null),
                new StructField("Grade 1", DataTypes.StringType, true, null),
                new StructField("Grade 2", DataTypes.StringType, true, null),
                new StructField("Grade 3", DataTypes.StringType, true, null),
                new StructField("Grade 4", DataTypes.StringType, true, null),
                new StructField("Grade 5", DataTypes.StringType, true, null),
                new StructField("Grade 6", DataTypes.StringType, true, null)
        };
        return new StructType(fields);
    }




}
