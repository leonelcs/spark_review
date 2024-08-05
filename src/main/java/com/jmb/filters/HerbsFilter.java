package com.jmb.filters;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class HerbsFilter implements FilterFunction<Row> {

    private static final String SUB_GROUP_COLUMN = "SUB GROUP";

    @Override
    public boolean call(Row row) throws Exception {
        //guarantee that will mach no matter the case
        return row.getAs(SUB_GROUP_COLUMN).toString().toLowerCase().equals("herbs");
    }
}