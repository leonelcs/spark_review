package com.jmb.mappers;

import com.jmb.model.Car;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;


public class CarMapper implements MapFunction<Row, Car> {

        @Override
        public Car call(Row carRow) throws Exception {
            Car car =  new Car();
            car.setId(carRow.getAs("Identification.ID"));
            car.setMake(carRow.getAs("Identification.Make"));
            car.setYear(carRow.getAs("Identification.Year"));
            car.setFuelType(carRow.getAs("Fuel_Information.Fuel_Type"));
            car.setTransmission(carRow.getAs("Engine_Information.Transmission"));
            return car;
        }
}
