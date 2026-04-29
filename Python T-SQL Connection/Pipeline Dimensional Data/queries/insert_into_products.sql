
-- ./insert_into_products.sql

-- Insert data into table: products
INSERT INTO ORDER_DDS.schema.products
   ([ProductID], [ProductName], [SupplierID], [CategoryID], [QuantityPerUnit], [UnitPrice], [UnitsInStock], [UnitsOnOrder], [ReorderLevel], [Discontinued])
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
