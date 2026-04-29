-- ================================================================
-- Script 1: Create Dimension and Fact Tables with Foreign Keys to Staging
-- ================================================================
USE ORDER_DDS;


-- ================================================================
-- 0) Drop Existing Tables in Correct Order
-- ================================================================
IF OBJECT_ID('dbo.OrderDetails','U')    IS NOT NULL DROP TABLE dbo.OrderDetails;
IF OBJECT_ID('dbo.FactOrders','U')      IS NOT NULL DROP TABLE dbo.FactOrders;
IF OBJECT_ID('dbo.DimProducts','U')     IS NOT NULL DROP TABLE dbo.DimProducts;
IF OBJECT_ID('dbo.DimCategories','U')   IS NOT NULL DROP TABLE dbo.DimCategories;
IF OBJECT_ID('dbo.DimCustomers','U')    IS NOT NULL DROP TABLE dbo.DimCustomers;
IF OBJECT_ID('dbo.DimEmployees','U')    IS NOT NULL DROP TABLE dbo.DimEmployees;
IF OBJECT_ID('dbo.DimTerritories','U')  IS NOT NULL DROP TABLE dbo.DimTerritories;
IF OBJECT_ID('dbo.DimSuppliers','U')    IS NOT NULL DROP TABLE dbo.DimSuppliers;
IF OBJECT_ID('dbo.DimShippers','U')     IS NOT NULL DROP TABLE dbo.DimShippers;
IF OBJECT_ID('dbo.DimRegion','U')       IS NOT NULL DROP TABLE dbo.DimRegion;
IF OBJECT_ID('dbo.Dim_SOR','U')         IS NOT NULL DROP TABLE dbo.Dim_SOR;



---------------------------------------------------------------------------
-- 1) CREATE DIMENSION AND FACT TABLES
---------------------------------------------------------------------------
CREATE TABLE dbo.DimCategories (
    CategoriesID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID_nk INT UNIQUE NOT NULL,
    CategoryName NVARCHAR(100) NOT NULL,
    [Description] NVARCHAR(MAX)
);


CREATE TABLE dbo.DimCustomers (
    CustomersID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID_nk NVARCHAR(10) UNIQUE NOT NULL,
    CompanyName NVARCHAR(100) NOT NULL,
    ContactName NVARCHAR(100),
    ContactTitle NVARCHAR(50),
    [Address] NVARCHAR(200),
    City NVARCHAR(50),
    Region NVARCHAR(50),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(50),
    Phone NVARCHAR(30),
    Fax NVARCHAR(30)
);


CREATE TABLE dbo.DimEmployees (
    EmployeeID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID_nk INT UNIQUE NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    FirstName NVARCHAR(50) NOT NULL,
    Title NVARCHAR(50),
    TitleOfCourtesy NVARCHAR(50),
    BirthDate DATETIME,
    HireDate DATETIME,
    [Address] NVARCHAR(200),
    City NVARCHAR(50),
    Region NVARCHAR(50),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(50),
    HomePhone NVARCHAR(30),
    Extension NVARCHAR(10),
    Notes NVARCHAR(MAX),
    ReportsTo INT NULL,
    PhotoPath NVARCHAR(200)
);


CREATE TABLE dbo.DimRegion (
    RegionID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    RegionID_nk INT UNIQUE NOT NULL,
    RegionDescription NVARCHAR(100) NOT NULL,
    RegionCategory NVARCHAR(50),
    RegionImportance NVARCHAR(50)
);


CREATE TABLE dbo.DimShippers (
    ShippersID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID_nk INT UNIQUE NOT NULL,
    CompanyName NVARCHAR(100) NOT NULL,
    Phone NVARCHAR(30)
);


CREATE TABLE dbo.DimSuppliers (
    SuppliersID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID_nk INT UNIQUE NOT NULL,
    CompanyName NVARCHAR(100) NOT NULL,
    ContactName NVARCHAR(100),
    ContactTitle NVARCHAR(50),
    [Address] NVARCHAR(200),
    City NVARCHAR(50),
    Region NVARCHAR(50),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(50),
    Phone NVARCHAR(30),
    Fax NVARCHAR(30),
    HomePage NVARCHAR(MAX)
);


CREATE TABLE dbo.DimTerritories (
    TerritoriesID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID_nk INT NOT NULL UNIQUE,
    TerritoryDescription NVARCHAR(100),
    TerritoryCode NVARCHAR(20),
    Region_sk_fk INT NOT NULL
);


CREATE TABLE dbo.DimProducts (
    ProductID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    ProductID_nk INT UNIQUE NOT NULL,
    ProductName NVARCHAR(100) NOT NULL,
    Supplier_sk_fk INT NOT NULL,
    Category_sk_fk INT NOT NULL,
    QuantityPerUnit NVARCHAR(50),
    UnitPrice DECIMAL(18,2),
    UnitsInStock SMALLINT,
    UnitsOnOrder SMALLINT,
    ReorderLevel SMALLINT,
    Discontinued BIT
);


CREATE TABLE dbo.FactOrders (
    FactOrdersID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    OrderID_nk INT UNIQUE NOT NULL,
    Customer_sk_fk INT NOT NULL,
    Employee_sk_fk INT NOT NULL,
    OrderDate DATETIME NOT NULL,
    RequiredDate DATETIME,
    ShippedDate DATETIME,
    ShipVia INT NOT NULL,
    Freight DECIMAL(18,2),
    ShipName NVARCHAR(100),
    ShipAddress NVARCHAR(200),
    ShipCity NVARCHAR(50),
    ShipRegion NVARCHAR(50),
    ShipPostalCode NVARCHAR(20),
    ShipCountry NVARCHAR(50),
    Territory_sk_fk INT NOT NULL
);


CREATE TABLE dbo.OrderDetails (
    OrderDetailsID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    Order_sk_fk INT NOT NULL,
    Product_sk_fk INT NOT NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    Quantity INT NOT NULL,
    Discount DECIMAL(5,2)
);


CREATE TABLE Dim_SOR (
    DimSORID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    StagingTableName NVARCHAR(100) NOT NULL,
    StagingRawID INT NOT NULL,
    LoadDateTime DATETIME DEFAULT GETDATE(),
    UNIQUE (StagingTableName, StagingRawID)
);


---------------------------------------------------------------------------
-- 2) ADD FOREIGN KEY CONSTRAINTS
---------------------------------------------------------------------------
ALTER TABLE dbo.DimEmployees
    ADD CONSTRAINT FK_DimEmployees_ReportsTo
    FOREIGN KEY (ReportsTo) REFERENCES dbo.DimEmployees(EmployeeID_sk_pk)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION;


ALTER TABLE dbo.DimTerritories
    ADD CONSTRAINT FK_DimTerritories_DimRegion
    FOREIGN KEY (Region_sk_fk) REFERENCES dbo.DimRegion(RegionID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.DimProducts
    ADD CONSTRAINT FK_DimProducts_DimSuppliers
    FOREIGN KEY (Supplier_sk_fk) REFERENCES dbo.DimSuppliers(SuppliersID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.DimProducts
    ADD CONSTRAINT FK_DimProducts_DimCategories
    FOREIGN KEY (Category_sk_fk) REFERENCES dbo.DimCategories(CategoriesID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.FactOrders
    ADD CONSTRAINT FK_FactOrders_DimCustomers
    FOREIGN KEY (Customer_sk_fk) REFERENCES dbo.DimCustomers(CustomersID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.FactOrders
    ADD CONSTRAINT FK_FactOrders_DimEmployees
    FOREIGN KEY (Employee_sk_fk) REFERENCES dbo.DimEmployees(EmployeeID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.FactOrders
    ADD CONSTRAINT FK_FactOrders_DimShippers
    FOREIGN KEY (ShipVia) REFERENCES dbo.DimShippers(ShippersID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.FactOrders
    ADD CONSTRAINT FK_FactOrders_DimTerritories
    FOREIGN KEY (Territory_sk_fk) REFERENCES dbo.DimTerritories(TerritoriesID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.OrderDetails
    ADD CONSTRAINT FK_OrderDetails_FactOrders
    FOREIGN KEY (Order_sk_fk) REFERENCES dbo.FactOrders(FactOrdersID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


ALTER TABLE dbo.OrderDetails
    ADD CONSTRAINT FK_OrderDetails_DimProducts
    FOREIGN KEY (Product_sk_fk) REFERENCES dbo.DimProducts(ProductID_sk_pk)
    ON DELETE CASCADE
    ON UPDATE CASCADE;


---------------------------------------------------------------------------
-- 3) ADD FOREIGN KEY CONSTRAINTS TO DIM TABLES FOR STAGING RAW IDs
---------------------------------------------------------------------------
-- Ensure that staging tables are created before adding these constraints.

-- 3.1) DimCategories Foreign Key to Categories_Staging
ALTER TABLE dbo.DimCategories
    ADD staging_raw_category_id INT NULL;


ALTER TABLE dbo.DimCategories
    ADD CONSTRAINT FK_DimCategories_Categories_Staging
    FOREIGN KEY (staging_raw_category_id) REFERENCES dbo.Categories_Staging(staging_raw_category_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


-- 3.2) DimCustomers Foreign Key to Customers_Staging
ALTER TABLE dbo.DimCustomers
    ADD staging_raw_customer_id INT NULL;


ALTER TABLE dbo.DimCustomers
    ADD CONSTRAINT FK_DimCustomers_Customers_Staging
    FOREIGN KEY (staging_raw_customer_id) REFERENCES dbo.Customers_Staging(staging_raw_customer_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


-- 3.3) DimEmployees Foreign Key to Employees_Staging
ALTER TABLE dbo.DimEmployees
    ADD staging_raw_employee_id INT NULL;


ALTER TABLE dbo.DimEmployees
    ADD CONSTRAINT FK_DimEmployees_Employees_Staging
    FOREIGN KEY (staging_raw_employee_id) REFERENCES dbo.Employees_Staging(staging_raw_employee_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


-- 3.4) DimRegion Foreign Key to Regions_Staging
ALTER TABLE dbo.DimRegion
    ADD staging_raw_region_id INT NULL;


ALTER TABLE dbo.DimRegion
    ADD CONSTRAINT FK_DimRegion_Regions_Staging
    FOREIGN KEY (staging_raw_region_id) REFERENCES dbo.Regions_Staging(staging_raw_region_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


-- 3.5) DimShippers Foreign Key to Shippers_Staging
ALTER TABLE dbo.DimShippers
    ADD staging_raw_shipper_id INT NULL;


ALTER TABLE dbo.DimShippers
    ADD CONSTRAINT FK_DimShippers_Shippers_Staging
    FOREIGN KEY (staging_raw_shipper_id) REFERENCES dbo.Shippers_Staging(staging_raw_shipper_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


-- 3.6) DimSuppliers Foreign Key to Suppliers_Staging
ALTER TABLE dbo.DimSuppliers
    ADD staging_raw_supplier_id INT NULL;


ALTER TABLE dbo.DimSuppliers
    ADD CONSTRAINT FK_DimSuppliers_Suppliers_Staging
    FOREIGN KEY (staging_raw_supplier_id) REFERENCES dbo.Suppliers_Staging(staging_raw_supplier_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


-- 3.7) DimTerritories Foreign Key to Territories_Staging
ALTER TABLE dbo.DimTerritories
    ADD staging_raw_territory_id INT NULL;


ALTER TABLE dbo.DimTerritories
    ADD CONSTRAINT FK_DimTerritories_Territories_Staging
    FOREIGN KEY (staging_raw_territory_id) REFERENCES dbo.Territories_Staging(staging_raw_territory_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


-- 3.8) DimProducts Foreign Key to Products_Staging
ALTER TABLE dbo.DimProducts
    ADD staging_raw_product_id INT NULL;


ALTER TABLE dbo.DimProducts
    ADD CONSTRAINT FK_DimProducts_Products_Staging
    FOREIGN KEY (staging_raw_product_id) REFERENCES dbo.Products_Staging(staging_raw_product_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE;


---------------------------------------------------------------------------
-- 4) FACT TABLE SNAPSHOT
---------------------------------------------------------------------------
MERGE dbo.FactOrders AS DST
USING (
    SELECT
        stg.OrderID AS OrderID_nk,
        stg.OrderDate,
        stg.RequiredDate,
        stg.ShippedDate,
        c.CustomersID_sk_pk    AS Customer_sk_fk,
        e.EmployeeID_sk_pk     AS Employee_sk_fk,
        shp.ShippersID_sk_pk   AS ShipVia,
        ter.TerritoriesID_sk_pk AS Territory_sk_fk,
        stg.Freight,
        stg.ShipName,
        stg.ShipAddress,
        stg.ShipCity,
        stg.ShipRegion,
        stg.ShipPostalCode,
        stg.ShipCountry
    FROM dbo.Orders_Staging AS stg
    LEFT JOIN dbo.DimCustomers    AS c   ON stg.CustomerID  = c.CustomerID_nk
    LEFT JOIN dbo.DimEmployees    AS e   ON stg.EmployeeID  = e.EmployeeID_nk
    LEFT JOIN dbo.DimShippers     AS shp ON stg.ShipVia     = shp.ShipperID_nk
    LEFT JOIN dbo.DimTerritories  AS ter ON stg.TerritoryID = ter.TerritoryID_nk
) AS SRC
ON DST.OrderID_nk = SRC.OrderID_nk

WHEN MATCHED AND (
       ISNULL(DST.Customer_sk_fk,  0) <> ISNULL(SRC.Customer_sk_fk,  0)
    OR ISNULL(DST.Employee_sk_fk,  0) <> ISNULL(SRC.Employee_sk_fk,  0)
    OR ISNULL(DST.ShipVia,         0) <> ISNULL(SRC.ShipVia,         0)
    OR ISNULL(DST.Territory_sk_fk, 0) <> ISNULL(SRC.Territory_sk_fk, 0)
    OR ISNULL(DST.OrderDate,       '1900-01-01') <> ISNULL(SRC.OrderDate,       '1900-01-01')
    OR ISNULL(DST.RequiredDate,    '1900-01-01') <> ISNULL(SRC.RequiredDate,    '1900-01-01')
    OR ISNULL(DST.ShippedDate,     '1900-01-01') <> ISNULL(SRC.ShippedDate,     '1900-01-01')
    OR ISNULL(DST.Freight,         0)            <> ISNULL(SRC.Freight,         0)
    OR ISNULL(DST.ShipName,        '')           <> ISNULL(SRC.ShipName,        '')
    OR ISNULL(DST.ShipAddress,     '')           <> ISNULL(SRC.ShipAddress,     '')
    OR ISNULL(DST.ShipCity,        '')           <> ISNULL(SRC.ShipCity,        '')
    OR ISNULL(DST.ShipRegion,      '')           <> ISNULL(SRC.ShipRegion,      '')
    OR ISNULL(DST.ShipPostalCode,  '')           <> ISNULL(SRC.ShipPostalCode,  '')
    OR ISNULL(DST.ShipCountry,     '')           <> ISNULL(SRC.ShipCountry,     '')
)
THEN
    UPDATE SET
       DST.Customer_sk_fk   = SRC.Customer_sk_fk,
       DST.Employee_sk_fk   = SRC.Employee_sk_fk,
       DST.ShipVia          = SRC.ShipVia,
       DST.Territory_sk_fk  = SRC.Territory_sk_fk,
       DST.OrderDate        = SRC.OrderDate,
       DST.RequiredDate     = SRC.RequiredDate,
       DST.ShippedDate      = SRC.ShippedDate,
       DST.Freight          = SRC.Freight,
       DST.ShipName         = SRC.ShipName,
       DST.ShipAddress      = SRC.ShipAddress,
       DST.ShipCity         = SRC.ShipCity,
       DST.ShipRegion       = SRC.ShipRegion,
       DST.ShipPostalCode   = SRC.ShipPostalCode,
       DST.ShipCountry      = SRC.ShipCountry

WHEN NOT MATCHED BY TARGET
THEN INSERT (
    OrderID_nk,
    Customer_sk_fk,
    Employee_sk_fk,
    ShipVia,
    Territory_sk_fk,
    OrderDate,
    RequiredDate,
    ShippedDate,
    Freight,
    ShipName,
    ShipAddress,
    ShipCity,
    ShipRegion,
    ShipPostalCode,
    ShipCountry
)
VALUES (
    SRC.OrderID_nk,
    SRC.Customer_sk_fk,
    SRC.Employee_sk_fk,
    SRC.ShipVia,
    SRC.Territory_sk_fk,
    SRC.OrderDate,
    SRC.RequiredDate,
    SRC.ShippedDate,
    SRC.Freight,
    SRC.ShipName,
    SRC.ShipAddress,
    SRC.ShipCity,
    SRC.ShipRegion,
    SRC.ShipPostalCode,
    SRC.ShipCountry
)

WHEN NOT MATCHED BY SOURCE
THEN DELETE;


---------------------------------------------------------------------------
-- 5) ADD FOREIGN KEY CONSTRAINT FOR Dim_SOR IF REQUIRED
---------------------------------------------------------------------------
-- If there are tables that should reference Dim_SOR, add foreign key constraints here.
-- Example:
-- ALTER TABLE dbo.SomeTable
--     ADD CONSTRAINT FK_SomeTable_Dim_SOR
--     FOREIGN KEY (SOR_sk_fk) REFERENCES dbo.Dim_SOR(SORID_sk_pk)
--     ON DELETE CASCADE
--     ON UPDATE CASCADE;


---------------------------------------------------------------------------
-- 6) SLOWLY CHANGING DIMENSIONS (SCD) HANDLING
---------------------------------------------------------------------------
-- (Include your existing SCD1, SCD2, SCD3, and SCD4 implementations here.)
-- Ensure that any table dependencies are resolved before executing this section.


---------------------------------------------------------------------------
-- END OF SCRIPT 1
---------------------------------------------------------------------------
