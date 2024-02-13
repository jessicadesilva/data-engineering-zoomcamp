 -- Fixes yellow table schema
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.yellow_tripdata`
    RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.yellow_tripdata`
    RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.yellow_tripdata`
    RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.yellow_tripdata`
    RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.yellow_tripdata`
    RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.yellow_tripdata`
    RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.yellow_tripdata`
    RENAME COLUMN dropoff_location_id TO DOLocationID;

 -- Fixes green table schema
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.green_tripdata`
    RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.green_tripdata`
    RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.green_tripdata`
    RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.green_tripdata`
    RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.green_tripdata`
    RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.green_tripdata`
    RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `taxi-rides-ny-339813-412521.trips_data_all.green_tripdata`
    RENAME COLUMN dropoff_location_id TO DOLocationID;