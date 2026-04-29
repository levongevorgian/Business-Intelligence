
-- ./insert_into_customers.sql

-- Insert data into table: customers
INSERT INTO ORDER_DDS.schema.customers
   ([CustomerID], [CompanyName], [ContactName], [ContactTitle], [Address], [City], [Region], [PostalCode], [Country], [Phone], [Fax])
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
