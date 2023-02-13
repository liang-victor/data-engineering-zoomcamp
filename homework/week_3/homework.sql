SELECT count(*) FROM `dtc-de-course-375316.dezoomcamp.fhv`

SELECT COUNT(DISTINCT affiliated_base_number) FROM `dtc-de-course-375316.dezoomcamp.fhv`
SELECT COUNT(DISTINCT affiliated_base_number) FROM `dtc-de-course-375316.dezoomcamp.fhv-ext`

SELECT count(*) FROM `dtc-de-course-375316.dezoomcamp.fhv`
where PUlocationID IS NULL and DOlocationID IS NULL;

CREATE OR REPLACE TABLE `dtc-de-course-375316.dezoomcamp.fhv-partitioned`
PARTITION BY
  DATE(pickup_datetime) 
  CLUSTER BY affiliated_base_number AS
SELECT * FROM `dtc-de-course-375316.dezoomcamp.fhv`

SELECT COUNT(DISTINCT affiliated_base_number) FROM `dtc-de-course-375316.dezoomcamp.fhv`
WHERE date(pickup_datetime)>='2019-03-01' AND date(pickup_datetime)<='2019-03-31'