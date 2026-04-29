
-- ./insert_into_suppliers.sql

-- Insert data into table: suppliers
INSERT INTO ORDER_DDS.schema.suppliers
   ([SupplierID], [CompanyName], [ContactName], [ContactTitle], [Address], [City], [Region], [PostalCode], [Country], [Phone], [Fax], [HomePage])
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
