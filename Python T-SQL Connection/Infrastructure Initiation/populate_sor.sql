-- Insert for Categories_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Categories_Staging', staging_raw_category_id
FROM dbo.Categories_Staging;


-- Insert for Customers_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Customers_Staging', staging_raw_customer_id
FROM dbo.Customers_Staging;


-- Insert for Employees_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Employees_Staging', staging_raw_employee_id
FROM dbo.Employees_Staging;


-- Insert for OrderDetails_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'OrderDetails_Staging', staging_raw_orderdetail_id
FROM dbo.OrderDetails_Staging;


-- Insert for Orders_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Orders_Staging', staging_raw_order_id
FROM dbo.Orders_Staging;


-- Insert for Products_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Products_Staging', staging_raw_product_id
FROM dbo.Products_Staging;


-- Insert for Regions_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Regions_Staging', staging_raw_region_id
FROM dbo.Regions_Staging;


-- Insert for Shippers_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Shippers_Staging', staging_raw_shipper_id
FROM dbo.Shippers_Staging;


-- Insert for Suppliers_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Suppliers_Staging', staging_raw_supplier_id
FROM dbo.Suppliers_Staging;


-- Insert for Territories_Staging
INSERT INTO Dim_SOR (StagingTableName, StagingRawID)
SELECT 'Territories_Staging', staging_raw_territory_id
FROM dbo.Territories_Staging;
