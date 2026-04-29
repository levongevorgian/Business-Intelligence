
-- ./insert_into_territories.sql

-- Insert data into table: territories
INSERT INTO ORDER_DDS.schema.territories
   ([TerritoryID], [TerritoryDescription], [TerritoryCode], [RegionID])
VALUES
    (?, ?, ?, ?);
