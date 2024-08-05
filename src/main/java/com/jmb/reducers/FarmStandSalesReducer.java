package com.jmb.reducers;

import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

public class FarmStandSalesReducer implements ReduceFunction<Row> {

        @Override
        public Row call(Row aggregatedRow, Row currentRow) throws Exception {
            GenericRowWithSchema updatedAggregatedRow = addCurrentRowValues(aggregatedRow, currentRow);
            return updatedAggregatedRow;
        }

        private GenericRowWithSchema addCurrentRowValues(Row aggregatedRow, Row currentRow) {
            Object[] rowValues = new Object[3];
            //Sum Total Sales
            rowValues[0] = Double.valueOf(aggregatedRow.getAs("total_sales").toString())
                    + Double.valueOf(currentRow.getAs("total_sales").toString());

            rowValues[1] = Double.valueOf(aggregatedRow.getAs("total_snap_sales").toString())
                    + Double.valueOf(currentRow.getAs("total_snap_sales").toString());

            rowValues[2] = Double.valueOf(aggregatedRow.getAs("total_double_sales").toString())
                    + Double.valueOf(currentRow.getAs("total_double_sales").toString());

            return new GenericRowWithSchema(rowValues, currentRow.schema());
        }


}
