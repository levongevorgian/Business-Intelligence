
-- ./insert_into_region.sql

-- Insert data into table: region
INSERT INTO ORDER_DDS.schema.region
   ([RegionID], [RegionDescription], [RegionCategory], [RegionImportance])
VALUES
    (?, ?, ?, ?);
