package com.spark.problems.model;

/**
 * Created by wyh669 on 1/22/16.
 */
public class CarsOwners {

    private Integer idCarsOwners;
    private Integer ownerId;
    private Integer carId;

    public Integer getIdCarsOwners() {
        return idCarsOwners;
    }

    public void setIdCarsOwners(Integer idCarsOwners) {
        this.idCarsOwners = idCarsOwners;
    }

    public Integer getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(Integer ownerId) {
        this.ownerId = ownerId;
    }

    public Integer getCarId() {
        return carId;
    }

    public void setCarId(Integer carId) {
        this.carId = carId;
    }
}
