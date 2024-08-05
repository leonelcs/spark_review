package com.jmb.mappers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

public class EmptyToUnderscoreMapper implements MapFunction<Row, Row> {

    final static String UNDERSCORE = "_";
    final static String BLANK = " ";
    final private String columnName;

    public EmptyToUnderscoreMapper(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public Row call(Row originalRow) throws Exception {
        String[] fieldNames = originalRow.schema().fieldNames();
        String[] newValues = populateRowValues(fieldNames, originalRow);

        return new GenericRowWithSchema(newValues, originalRow.schema());
    }

    private String[] populateRowValues(String[] fieldNames, Row originalRow) {
        String[] newValues = new String[fieldNames.length];

        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(columnName)) {
                newValues[i] = ((String) originalRow.getAs(columnName))
                        .replace(BLANK, UNDERSCORE);
            } else {
                newValues[i] = (String) originalRow.get(i);
            }
        }
        return newValues;
    }

}
