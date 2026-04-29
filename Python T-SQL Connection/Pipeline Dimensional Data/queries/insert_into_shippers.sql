
-- ./insert_into_shippers.sql

-- Insert data into table: shippers
INSERT INTO ORDER_DDS.schema.shippers
   ([ShipperID], [CompanyName], [Phone])
VALUES
    (?, ?, ?);
