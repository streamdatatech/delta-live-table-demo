# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime,timezone

# COMMAND ----------

@dlt.table(
    comment="Customer Bronze Table",
    table_properties={"quality": "bronze"},
    path="/mnt/bronze/TripManagement/Customers",
)
def customers_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/customers/")
        .withColumn("IngestionDateTime", lit(datetime.now(timezone.utc)))
    )


@dlt.table(
    comment="Customer Silver Table",
    table_properties={"quality": "silver"},
    path="/mnt/silver/TripManagement/Customers",
)
def customers_silver():
    return (
        dlt.read("customers_bronze")
        .select(
            col("name").alias("Name"),
            col("email").alias("Email"),
            col("gender").alias("Gender"),
            col("phone").cast("double").alias("Phone"),
            col("date_created").cast("timestamp").alias("DateCreated"),
        )
        .withColumn("IngestionDateTime", lit(datetime.now(timezone.utc)))
    )

# COMMAND ----------

@dlt.table(
    comment="Driver Bronze Table",
    table_properties={"quality": "bronze"},
    path="/mnt/bronze/TripManagement/Drivers",
)
def drivers_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/drivers/")
        .withColumn("IngestionDateTime", lit(datetime.now(timezone.utc)))
    )


@dlt.table(
    comment="Driver Silver Table",
    table_properties={"quality": "silver"},
    path="/mnt/silver/TripManagement/Drivers",
)
def drivers_silver():
    return (
        dlt.read("drivers_bronze")
        .select(
            col("name").alias("Name"),
            col("email").alias("Email"),
            col("gender").alias("Gender"),
            col("phone").cast("double").alias("Phone"),
            col("car_reg").alias("CarRegistrationNo"),
            col("date_created").cast("timestamp").alias("DateCreated"),
        )
        .withColumn("IngestionDateTime", lit(datetime.now(timezone.utc)))
    )

# COMMAND ----------

@dlt.table(
    comment="Trip Bronze Table",
    table_properties={"quality": "bronze"},
    path="/mnt/bronze/TripManagement/Trips",
)
def trips_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/trips/")
        .withColumn("IngestionDateTime", lit(datetime.now(timezone.utc)))
    )


@dlt.table(
    comment="Trip Silver Table",
    table_properties={"quality": "silver"},
    path="/mnt/silver/TripManagement/Trips",
)
def trips_silver():
    return (
        dlt.read("trips_bronze")
        .select(
            col("id").cast("double").alias("Id"),
            col("car_reg").alias("CarRegistrationNo"),
            col("customer_email").alias("CustomerEmail"),
            col("origin").alias("Origin"),
            col("destination").alias("Destination"),
            col("start_time").cast("timestamp").alias("TripStartTime"),
            col("total_distance").cast("double").alias("TotalDistanceMeters"),
            col("completed_distance").cast("double").alias("CompletedDistanceMeters"),
            col("status").alias("TripsStatus"),
        )
        .withColumn("IngestionDateTime", lit(datetime.now(timezone.utc)))
    )

# COMMAND ----------

@dlt.table(
    comment="Trip Gold Table",
    table_properties={"quality": "gold"},
    path="/mnt/gold/TripManagement/Trips",
)
def trips_gold():
    return (
        dlt.read("trips_silver")
        .alias("t")
        .join(
            dlt.read("drivers_silver").alias("d"),
            col("t.CarRegistrationNo") == col("d.CarRegistrationNo"),
        )
        .join(
            dlt.read("customers_silver").alias("c"),
            col("t.CustomerEmail") == col("c.Email"),
        )
        .select(            
            col("Id"),
            col("t.CarRegistrationNo"),
            col("d.Name").alias("DriverName"),
            col("d.Email").alias("DriverEmail"),
            col("d.Gender").alias("DriverGender"),
            col("d.Phone").alias("DriverPhone"),
            col("c.Name").alias("CustomerName"),
            col("c.Email").alias("CustomerEmail"),
            col("c.Gender").alias("CustomerGender"),
            col("c.Phone").alias("CustomerPhone"),
            col("Origin"),
            col("Destination"),
            col("TripStartTime"),
            col("TotalDistanceMeters"),
            col("CompletedDistanceMeters"),
            col("TripsStatus").alias("TripsStatus")
        )
    )
