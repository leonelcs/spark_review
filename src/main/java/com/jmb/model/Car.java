package com.jmb.model;

import lombok.Data;

/**
 * Pojo class for Car
 * - note: records are not supported by mapping frameworks like MapStruct
 */
@Data
public class Car {

    String id;
    String make;
    String year;
    String transmission;
    String fuelType;
}
