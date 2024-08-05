package com.jmb;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Joins {

    private static final Logger LOGGER = LoggerFactory.getLogger(Joins.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    private static final String PATH_RESOURCES_DF1 = "src/main/resources/spark-data/employees.csv";
    private static final String PATH_RESOURCES_DF2 = "src/main/resources/spark-data/departments.csv";

    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        Joins app = new Joins();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    private void init() {

        SparkSession session = SparkSession.builder()
                .appName("Joins")
                .master("local").getOrCreate();

        Dataset<Row> employeeDf = session.read().format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PATH_RESOURCES_DF1);

        employeeDf.show();

        Dataset<Row> departmentDf = session.read().format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PATH_RESOURCES_DF2);

        departmentDf.show();

        //Inner Join
        Dataset<Row> joinedDfs = employeeDf.join(departmentDf, employeeDf.col("emp_dept_id")
                .equalTo(departmentDf.col("dept_id")), "inner");

        //Drop unwanted columns for this query
        Dataset<Row> simpleJoinedDf = joinedDfs.drop("manager_emp_id", "start_year", "gender", "salary");

        //Show the joined DataFrames
        simpleJoinedDf.show();

        /** LEFT (OUTER) JOIN - List employees and the departments they belong to*/
        Dataset<Row> employeesDepts =
                employeeDf
                        .join(departmentDf, employeeDf.col("emp_dept_id").equalTo(departmentDf.col("dept_id")), "leftouter");

        employeesDepts.show();

        /** RIGHT (OUTER) JOIN - List departments and employees they belong to, if any*/
        Dataset<Row> deptsEmployees =
                employeeDf.join(departmentDf, employeeDf.col("emp_dept_id").equalTo(departmentDf.col("dept_id")), "rightouter");

        deptsEmployees.drop("start_year","gender", "salary").show();

        /** FULL (OUTER) JOIN - List employees and the departments they belong to*/
        Dataset<Row> empsDeptsFull =
                employeeDf.join(departmentDf,
                        employeeDf.col("emp_dept_id").equalTo(departmentDf.col("dept_id")), "fullouter");

        empsDeptsFull.drop("start_year","gender", "salary").show();

        /** SELF (INNER) JOIN - List the employees next to their managers - Uses SparkSQL Component */

        //Create global view to use in SQL like syntax
        employeeDf.createOrReplaceTempView("employees");

        //Do the inner join
        Dataset<Row> empAndManagers =
                session.sql("SELECT E1.emp_id, E1.name, E1.manager_emp_id as manager_id, " +
                        "E2.emp_id as manager_emp_id, E2.name as manager_name " +
                        "FROM employees AS E1, employees AS E2 " +
                        "WHERE E1.manager_emp_id = E2.emp_id ");

        //List above DataFrame records
        empAndManagers.show();

    }
}
