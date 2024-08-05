package com.jmb.mappers;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import scala.collection.mutable.ArraySeq;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;


public class StudentGradesFlatMapper implements FlatMapFunction<Row, Row> {


    @Override
    public Iterator<Row> call(Row row) throws Exception {
        Row newRow = new GenericRowWithSchema(mapToNewColumns(row), row.schema());
        return Arrays.asList(newRow).iterator();
    }

    private Object[] mapToNewColumns(Row row) {
        ArraySeq<String> seq = row.getAs("Grades");

        List<String> list = new ArrayList<>();
        list.add(row.getAs("Student Name").toString());
        int finalRowSize = seq.size()+2;
        Object[] finalRow = new Object[finalRowSize];
        int rowColsIndex = 0;
        finalRow[rowColsIndex++] = row.getAs("Student Name");

        seq.map(item -> {
            String temp = String.valueOf(item);
            if (Integer.parseInt(temp) < 6) {
                list.add("R");
            } else {
                list.add(temp);
            }
            return item;
        });

        for(String item : list) {
            finalRow[rowColsIndex++] = item;
        }

        return finalRow;
    }
}
