USE ORDER_DDS;

IF DB_ID('ORDER_DDS') IS NULL
BEGIN
    CREATE DATABASE ORDER_DDS;
END

USE ORDER_DDS;

IF OBJECT_ID('dbo.Categories_Staging','U') IS NOT NULL DROP TABLE dbo.Categories_Staging;
IF OBJECT_ID('dbo.Customers_Staging','U')  IS NOT NULL DROP TABLE dbo.Customers_Staging;
IF OBJECT_ID('dbo.Employees_Staging','U')  IS NOT NULL DROP TABLE dbo.Employees_Staging;
IF OBJECT_ID('dbo.OrderDetails_Staging','U') IS NOT NULL DROP TABLE dbo.OrderDetails_Staging;
IF OBJECT_ID('dbo.Orders_Staging','U')     IS NOT NULL DROP TABLE dbo.Orders_Staging;
IF OBJECT_ID('dbo.Products_Staging','U')   IS NOT NULL DROP TABLE dbo.Products_Staging;
IF OBJECT_ID('dbo.Regions_Staging','U')    IS NOT NULL DROP TABLE dbo.Regions_Staging;
IF OBJECT_ID('dbo.Shippers_Staging','U')   IS NOT NULL DROP TABLE dbo.Shippers_Staging;
IF OBJECT_ID('dbo.Suppliers_Staging','U')  IS NOT NULL DROP TABLE dbo.Suppliers_Staging;
IF OBJECT_ID('dbo.Territories_Staging','U') IS NOT NULL DROP TABLE dbo.Territories_Staging;

CREATE TABLE Categories_Staging (
    staging_raw_category_id INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT,
    CategoryName NVARCHAR(100),
    Description NVARCHAR(MAX)
);

CREATE TABLE Customers_Staging (
    staging_raw_customer_id INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(10),
    CompanyName NVARCHAR(100),
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

CREATE TABLE Employees_Staging (
    staging_raw_employee_id INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    LastName NVARCHAR(50),
    FirstName NVARCHAR(50),
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
    Photo VARBINARY(MAX),
    Notes NVARCHAR(MAX),
    ReportsTo INT,
    PhotoPath NVARCHAR(200)
);

CREATE TABLE OrderDetails_Staging (
    staging_raw_orderdetail_id INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    UnitPrice DECIMAL(10,2),
    Quantity INT,
    Discount DECIMAL(5,2)
);

CREATE TABLE Orders_Staging (
    staging_raw_order_id INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    CustomerID NVARCHAR(10),
    EmployeeID INT,
    OrderDate DATETIME,
    RequiredDate DATETIME,
    ShippedDate DATETIME,
    ShipVia INT,
    Freight DECIMAL(10,2),
    ShipName NVARCHAR(100),
    ShipAddress NVARCHAR(200),
    ShipCity NVARCHAR(50),
    ShipRegion NVARCHAR(50),
    ShipPostalCode NVARCHAR(20),
    ShipCountry NVARCHAR(50),
    TerritoryID INT
);

CREATE TABLE Products_Staging (
    staging_raw_product_id INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName NVARCHAR(100),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit NVARCHAR(50),
    UnitPrice DECIMAL(10,2),
    UnitsInStock INT,
    UnitsOnOrder INT,
    ReorderLevel INT,
    Discontinued BIT
);

CREATE TABLE Regions_Staging (
    staging_raw_region_id INT IDENTITY(1,1) PRIMARY KEY,
    RegionDescription NVARCHAR(100)
);

CREATE TABLE Shippers_Staging (
    staging_raw_shipper_id INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID INT,
    CompanyName NVARCHAR(100),
    Phone NVARCHAR(30)
);

CREATE TABLE Suppliers_Staging (
    staging_raw_supplier_id INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT,
    CompanyName NVARCHAR(100),
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

CREATE TABLE Territories_Staging (
    staging_raw_territory_id INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID INT,
    TerritoryDescription NVARCHAR(50),
    TerritoryCode NVARCHAR(20),
    RegionID INT
);
