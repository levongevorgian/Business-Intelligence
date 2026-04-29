USE ORDER_DDS;

-- Declare date variables at the top so they can be reused
DECLARE @Yesterday INT = (YEAR(DATEADD(DAY, -1, GETDATE())) * 10000)
                       + (MONTH(DATEADD(DAY, -1, GETDATE())) * 100)
                       + DAY(DATEADD(DAY, -1, GETDATE()));
DECLARE @Today INT = (YEAR(GETDATE()) * 10000)
                    + (MONTH(GETDATE()) * 100)
                    + DAY(GETDATE());

---------------------------------------------------------------------------
-- DimCategories SCD 1 with Delete
---------------------------------------------------------------------------
DROP TABLE IF EXISTS dbo.DimCategories_SCD1;

CREATE TABLE dbo.DimCategories_SCD1 (
    CategoriesID_sk_pk INT PRIMARY KEY IDENTITY(1, 1),
    CategoryID_nk      INT,
    CategoryName       NVARCHAR(255) NOT NULL,
    [Description]      VARCHAR(500)  NULL
);

MERGE dbo.DimCategories_SCD1 AS DST
USING dbo.Categories_Staging AS SRC
    ON (DST.CategoryID_nk = SRC.CategoryID)
WHEN MATCHED
    AND (
       ISNULL(DST.CategoryName, '') <> ISNULL(SRC.CategoryName, '')
       OR ISNULL(DST.[Description], '') <> ISNULL(SRC.[Description], '')
    )
THEN
    UPDATE
    SET
        DST.CategoryName  = SRC.CategoryName,
        DST.[Description] = SRC.[Description]
WHEN NOT MATCHED BY TARGET
THEN
    INSERT (CategoryID_nk, CategoryName, [Description])
    VALUES (SRC.CategoryID, SRC.CategoryName, SRC.[Description])
WHEN NOT MATCHED BY SOURCE
THEN
    DELETE;

SELECT * FROM dbo.DimCategories_SCD1;

---------------------------------------------------------------------------
-- DimCustomers SCD 2
---------------------------------------------------------------------------
DROP TABLE IF EXISTS dbo.DimCustomers_SCD2;
CREATE TABLE dbo.DimCustomers_SCD2
(
    DimCustomersSK INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID_nk  NVARCHAR(10)  NOT NULL,
    CompanyName    NVARCHAR(100) NOT NULL,
    ContactName    NVARCHAR(100),
    ContactTitle   NVARCHAR(50),
    [Address]      NVARCHAR(200),
    City           NVARCHAR(50),
    Region         NVARCHAR(50),
    PostalCode     NVARCHAR(20),
    Country        NVARCHAR(50),
    Phone          NVARCHAR(30),
    Fax            NVARCHAR(30),
    ValidFrom      INT,
    ValidTo        INT,
    IsCurrent      BIT
);

-- Ensure the staging table exists
IF OBJECT_ID('dbo.Customers_Staging','U') IS NULL
BEGIN
    CREATE TABLE dbo.Customers_Staging
    (
        CustomerID   NVARCHAR(10),
        CompanyName  NVARCHAR(100),
        ContactName  NVARCHAR(100),
        ContactTitle NVARCHAR(50),
        [Address]    NVARCHAR(200),
        City         NVARCHAR(50),
        Region       NVARCHAR(50),
        PostalCode   NVARCHAR(20),
        Country      NVARCHAR(50),
        Phone        NVARCHAR(30),
        Fax          NVARCHAR(30)
    );
END;

-- Temporary table to capture updates
IF OBJECT_ID('tempdb..#DimCustomersUpdates') IS NOT NULL
    DROP TABLE #DimCustomersUpdates;

CREATE TABLE #DimCustomersUpdates
(
    ActionTaken   NVARCHAR(10),
    CustomerID    NVARCHAR(10),
    CompanyName   NVARCHAR(100),
    ContactName   NVARCHAR(100),
    ContactTitle  NVARCHAR(50),
    [Address]     NVARCHAR(200),
    City          NVARCHAR(50),
    Region        NVARCHAR(50),
    PostalCode    NVARCHAR(20),
    Country       NVARCHAR(50),
    Phone         NVARCHAR(30),
    Fax           NVARCHAR(30)
);

MERGE dbo.DimCustomers_SCD2 AS DST
USING dbo.Customers_Staging AS SRC
    ON DST.CustomerID_nk = SRC.CustomerID
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        CustomerID_nk,
        CompanyName,
        ContactName,
        ContactTitle,
        [Address],
        City,
        Region,
        PostalCode,
        Country,
        Phone,
        Fax,
        ValidFrom,
        IsCurrent
    )
    VALUES (
        SRC.CustomerID,
        SRC.CompanyName,
        SRC.ContactName,
        SRC.ContactTitle,
        SRC.[Address],
        SRC.City,
        SRC.Region,
        SRC.PostalCode,
        SRC.Country,
        SRC.Phone,
        SRC.Fax,
        @Today,
        1
    )
WHEN MATCHED
     AND DST.IsCurrent = 1
     AND (
            ISNULL(DST.CompanyName,'') <> ISNULL(SRC.CompanyName,'')
         OR ISNULL(DST.ContactName,'') <> ISNULL(SRC.ContactName,'')
         OR ISNULL(DST.ContactTitle,'') <> ISNULL(SRC.ContactTitle,'')
         OR ISNULL(DST.[Address],'') <> ISNULL(SRC.[Address],'')
         OR ISNULL(DST.City,'') <> ISNULL(SRC.City,'')
         OR ISNULL(DST.Region,'') <> ISNULL(SRC.Region,'')
         OR ISNULL(DST.PostalCode,'') <> ISNULL(SRC.PostalCode,'')
         OR ISNULL(DST.Country,'') <> ISNULL(SRC.Country,'')
         OR ISNULL(DST.Phone,'') <> ISNULL(SRC.Phone,'')
         OR ISNULL(DST.Fax,'') <> ISNULL(SRC.Fax,'')
        )
THEN
    UPDATE
        SET DST.IsCurrent = 0,
            DST.ValidTo   = @Yesterday
    OUTPUT
        $action,
        SRC.CustomerID,
        SRC.CompanyName,
        SRC.ContactName,
        SRC.ContactTitle,
        SRC.[Address],
        SRC.City,
        SRC.Region,
        SRC.PostalCode,
        SRC.Country,
        SRC.Phone,
        SRC.Fax
    INTO #DimCustomersUpdates;

INSERT INTO dbo.DimCustomers_SCD2
(
    CustomerID_nk,
    CompanyName,
    ContactName,
    ContactTitle,
    [Address],
    City,
    Region,
    PostalCode,
    Country,
    Phone,
    Fax,
    ValidFrom,
    IsCurrent
)
SELECT
    U.CustomerID,
    U.CompanyName,
    U.ContactName,
    U.ContactTitle,
    U.[Address],
    U.City,
    U.Region,
    U.PostalCode,
    U.Country,
    U.Phone,
    U.Fax,
    @Today,
    1
FROM #DimCustomersUpdates U
WHERE U.ActionTaken = 'UPDATE';

SELECT * FROM dbo.DimCustomers_SCD2;

---------------------------------------------------------------------------
-- SCD4: DimEmployees and DimEmployees_SCD4_History
---------------------------------------------------------------------------
IF OBJECT_ID('dbo.DimEmployees_SCD4_History', 'U') IS NOT NULL
    DROP TABLE dbo.DimEmployees_SCD4_History;

CREATE TABLE dbo.DimEmployees_SCD4_History (
    EmployeeID_sk_pk INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID_nk INT NOT NULL,
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
    PhotoPath NVARCHAR(200),
    EndDate DATE NULL
);

DECLARE @Today_Employees DATE = CAST(GETDATE() AS DATE);

IF OBJECT_ID('tempdb..#TempSCD4Output', 'U') IS NOT NULL
    DROP TABLE #TempSCD4Output;

CREATE TABLE #TempSCD4Output (
    MergeAction NVARCHAR(10),
    EmployeeID_nk INT,
    InsertedSK INT
);

MERGE dbo.DimEmployees AS DST
USING dbo.Employees_Staging AS SRC
    ON SRC.EmployeeID = DST.EmployeeID_nk
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        EmployeeID_nk,
        LastName,
        FirstName,
        Title,
        TitleOfCourtesy,
        BirthDate,
        HireDate,
        [Address],
        City,
        Region,
        PostalCode,
        Country,
        HomePhone,
        Extension,
        Notes,
        ReportsTo,
        PhotoPath
    )
    VALUES (
        SRC.EmployeeID,
        SRC.LastName,
        SRC.FirstName,
        SRC.Title,
        SRC.TitleOfCourtesy,
        SRC.BirthDate,
        SRC.HireDate,
        SRC.[Address],
        SRC.City,
        SRC.Region,
        SRC.PostalCode,
        SRC.Country,
        SRC.HomePhone,
        SRC.Extension,
        SRC.Notes,
        SRC.ReportsTo,
        SRC.PhotoPath
    )
WHEN MATCHED AND (
    ISNULL(DST.LastName, '')    <> ISNULL(SRC.LastName, '')  OR
    ISNULL(DST.FirstName, '')   <> ISNULL(SRC.FirstName, '') OR
    ISNULL(DST.Title, '')       <> ISNULL(SRC.Title, '')     OR
    ISNULL(DST.TitleOfCourtesy, '') <> ISNULL(SRC.TitleOfCourtesy, '') OR
    ISNULL(DST.BirthDate, '1900-01-01') <> ISNULL(SRC.BirthDate, '1900-01-01') OR
    ISNULL(DST.HireDate, '1900-01-01')  <> ISNULL(SRC.HireDate, '1900-01-01')  OR
    ISNULL(DST.[Address], '')   <> ISNULL(SRC.[Address], '') OR
    ISNULL(DST.City, '')        <> ISNULL(SRC.City, '')      OR
    ISNULL(DST.Region, '')      <> ISNULL(SRC.Region, '')    OR
    ISNULL(DST.PostalCode, '')  <> ISNULL(SRC.PostalCode, '') OR
    ISNULL(DST.Country, '')     <> ISNULL(SRC.Country, '')   OR
    ISNULL(DST.HomePhone, '')   <> ISNULL(SRC.HomePhone, '') OR
    ISNULL(DST.Extension, '')   <> ISNULL(SRC.Extension, '') OR
    ISNULL(DST.Notes, '')       <> ISNULL(SRC.Notes, '')     OR
    ISNULL(DST.ReportsTo, 0)    <> ISNULL(SRC.ReportsTo, 0)  OR
    ISNULL(DST.PhotoPath, '')   <> ISNULL(SRC.PhotoPath, '')
)
THEN
    UPDATE
        SET
            LastName = SRC.LastName,
            FirstName = SRC.FirstName,
            Title = SRC.Title,
            TitleOfCourtesy = SRC.TitleOfCourtesy,
            BirthDate = SRC.BirthDate,
            HireDate = SRC.HireDate,
            [Address] = SRC.[Address],
            City = SRC.City,
            Region = SRC.Region,
            PostalCode = SRC.PostalCode,
            Country = SRC.Country,
            HomePhone = SRC.HomePhone,
            Extension = SRC.Extension,
            Notes = SRC.Notes,
            ReportsTo = SRC.ReportsTo,
            PhotoPath = SRC.PhotoPath
OUTPUT
    $action,
    DELETED.EmployeeID_nk,
    INSERTED.EmployeeID_sk_pk
INTO #TempSCD4Output(MergeAction, EmployeeID_nk, InsertedSK);

INSERT INTO dbo.DimEmployees_SCD4_History (
    EmployeeID_nk, LastName, FirstName, Title, TitleOfCourtesy,
    BirthDate, HireDate, [Address], City, Region, PostalCode, Country,
    HomePhone, Extension, Notes, ReportsTo, PhotoPath, EndDate
)
SELECT
    DST.EmployeeID_nk,
    DST.LastName,
    DST.FirstName,
    DST.Title,
    DST.TitleOfCourtesy,
    DST.BirthDate,
    DST.HireDate,
    DST.[Address],
    DST.City,
    DST.Region,
    DST.PostalCode,
    DST.Country,
    DST.HomePhone,
    DST.Extension,
    DST.Notes,
    DST.ReportsTo,
    DST.PhotoPath,
    @Today_Employees
FROM dbo.DimEmployees AS DST
INNER JOIN #TempSCD4Output AS T
    ON DST.EmployeeID_nk = T.EmployeeID_nk
WHERE T.MergeAction = 'UPDATE';

DROP TABLE #TempSCD4Output;

SELECT * FROM dbo.DimEmployees;
SELECT * FROM dbo.DimEmployees_SCD4_History;

---------------------------------------------------------------------------
-- DimProducts SCD 4
---------------------------------------------------------------------------
IF OBJECT_ID('dbo.DimProducts_History', 'U') IS NOT NULL DROP TABLE dbo.DimProducts_History;

CREATE TABLE DimProducts_History (
    HistoryID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID_nk INT NOT NULL,
    ProductName NVARCHAR(100),
    Supplier_sk_fk INT,
    Category_sk_fk INT,
    QuantityPerUnit NVARCHAR(50),
    UnitPrice DECIMAL(18,2),
    UnitsInStock SMALLINT,
    UnitsOnOrder SMALLINT,
    ReorderLevel SMALLINT,
    Discontinued BIT,
    ChangeTimestamp DATETIME DEFAULT GETDATE(),
    ValidTo DATETIME
);

DECLARE @Today_Products DATE = GETDATE();

IF OBJECT_ID('tempdb..#TempHistory', 'U') IS NOT NULL DROP TABLE #TempHistory;

CREATE TABLE #TempHistory (
    ProductID_nk INT NOT NULL,
    ProductName NVARCHAR(100),
    Supplier_sk_fk INT,
    Category_sk_fk INT,
    QuantityPerUnit NVARCHAR(50),
    UnitPrice DECIMAL(18,2),
    UnitsInStock SMALLINT,
    UnitsOnOrder SMALLINT,
    ReorderLevel SMALLINT,
    Discontinued BIT,
    ChangeTimestamp DATETIME DEFAULT GETDATE(),
    ValidTo DATETIME
);

MERGE dbo.DimProducts AS Target
USING dbo.Products_Staging AS Source
    ON Target.ProductID_nk = Source.ProductID
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ProductID_nk, ProductName, Supplier_sk_fk, Category_sk_fk, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)
    VALUES (Source.ProductID, Source.ProductName, Source.SupplierID, Source.CategoryID, Source.QuantityPerUnit, Source.UnitPrice, Source.UnitsInStock, Source.UnitsOnOrder, Source.ReorderLevel, Source.Discontinued)
WHEN MATCHED AND (
       ISNULL(Target.ProductName, '') <> ISNULL(Source.ProductName, '')
    OR ISNULL(Target.Supplier_sk_fk, 0) <> ISNULL(Source.SupplierID, 0)
    OR ISNULL(Target.Category_sk_fk, 0) <> ISNULL(Source.CategoryID, 0)
    OR ISNULL(Target.QuantityPerUnit, '') <> ISNULL(Source.QuantityPerUnit, '')
    OR ISNULL(Target.UnitPrice, 0) <> ISNULL(Source.UnitPrice, 0)
    OR ISNULL(Target.UnitsInStock, 0) <> ISNULL(Source.UnitsInStock, 0)
    OR ISNULL(Target.UnitsOnOrder, 0) <> ISNULL(Source.UnitsOnOrder, 0)
    OR ISNULL(Target.ReorderLevel, 0) <> ISNULL(Source.ReorderLevel, 0)
    OR ISNULL(Target.Discontinued, 0) <> ISNULL(Source.Discontinued, 0)
) THEN
    UPDATE SET
        Target.ProductName = Source.ProductName,
        Target.Supplier_sk_fk = Source.SupplierID,
        Target.Category_sk_fk = Source.CategoryID,
        Target.QuantityPerUnit = Source.QuantityPerUnit,
        Target.UnitPrice = Source.UnitPrice,
        Target.UnitsInStock = Source.UnitsInStock,
        Target.UnitsOnOrder = Source.UnitsOnOrder,
        Target.ReorderLevel = Source.ReorderLevel,
        Target.Discontinued = Source.Discontinued
OUTPUT
    DELETED.ProductID_nk,
    DELETED.ProductName,
    DELETED.Supplier_sk_fk,
    DELETED.Category_sk_fk,
    DELETED.QuantityPerUnit,
    DELETED.UnitPrice,
    DELETED.UnitsInStock,
    DELETED.UnitsOnOrder,
    DELETED.ReorderLevel,
    DELETED.Discontinued,
    GETDATE() AS ChangeTimestamp,
    NULL AS ValidTo
INTO #TempHistory;

INSERT INTO dbo.DimProducts_History
(ProductID_nk, ProductName, Supplier_sk_fk, Category_sk_fk, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued, ChangeTimestamp, ValidTo)
SELECT
    ProductID_nk, ProductName, Supplier_sk_fk, Category_sk_fk, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued, ChangeTimestamp, ValidTo
FROM #TempHistory;

DROP TABLE #TempHistory;

SELECT * FROM dbo.DimProducts;
SELECT * FROM dbo.DimProducts_History;

---------------------------------------------------------------------------
-- DimRegion SCD3
---------------------------------------------------------------------------
DROP TABLE IF EXISTS [dbo].[DimRegion_SCD3];

CREATE TABLE [dbo].[DimRegion_SCD3] (
    RegionID_SK_PK INT PRIMARY KEY IDENTITY(1,1),
    RegionID_NK INT NOT NULL UNIQUE,
    CurrentRegionDescription NVARCHAR(100) NOT NULL,
    PriorRegionDescription NVARCHAR(100) NULL
);

-- Merge data into DimRegion_SCD3
MERGE INTO dbo.DimRegion_SCD3 AS Target
USING dbo.DimRegion AS Source
    ON Target.RegionID_NK = Source.RegionID_NK
WHEN NOT MATCHED BY TARGET THEN
    INSERT (RegionID_NK, CurrentRegionDescription)
    VALUES (Source.RegionID_NK, Source.RegionDescription)
WHEN MATCHED AND ISNULL(Target.CurrentRegionDescription, '') <> ISNULL(Source.RegionDescription, '')
THEN
    UPDATE SET
        Target.PriorRegionDescription = Target.CurrentRegionDescription,
        Target.CurrentRegionDescription = Source.RegionDescription;

SELECT * FROM dbo.DimRegion_SCD3;

---------------------------------------------------------------------------
-- DimShippers SCD1
---------------------------------------------------------------------------
DROP TABLE IF EXISTS DimShippers_SCD1;

CREATE TABLE DimShippers_SCD1 (
    ShipperID_PK_SK INT PRIMARY KEY IDENTITY(1, 1),
    ShipperID_NK INT NOT NULL UNIQUE,
    CompanyName NVARCHAR(255) NOT NULL,
    Phone NVARCHAR(24) NULL
);

MERGE DimShippers_SCD1 AS DST
USING dbo.Shippers_Staging AS SRC
    ON (SRC.ShipperID = DST.ShipperID_NK)
WHEN MATCHED AND (
    ISNULL(DST.CompanyName, '') <> ISNULL(SRC.CompanyName, '') OR
    ISNULL(DST.Phone, '') <> ISNULL(SRC.Phone, '')
)
THEN
    UPDATE
    SET
        DST.CompanyName = SRC.CompanyName,
        DST.Phone = SRC.Phone
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ShipperID_NK, CompanyName, Phone)
    VALUES (SRC.ShipperID, SRC.CompanyName, SRC.Phone);

SELECT * FROM DimShippers_SCD1;

---------------------------------------------------------------------------
-- DimSuppliers SCD4
---------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'DimSuppliers_SCD4_History'
)
BEGIN
    CREATE TABLE DimSuppliers_SCD4_History (
        SupplierID_PK_SK INT PRIMARY KEY IDENTITY(1, 1),
        SupplierID_NK INT NOT NULL,
        CompanyName NVARCHAR(255) NOT NULL,
        ContactName NVARCHAR(100),
        ContactTitle NVARCHAR(100),
        Address NVARCHAR(255),
        City NVARCHAR(100),
        Region NVARCHAR(100),
        PostalCode NVARCHAR(20),
        Country NVARCHAR(100),
        Phone NVARCHAR(50),
        Fax NVARCHAR(50),
        HomePage NVARCHAR(255),
        ArchiveDate DATE NOT NULL
    );
END;

DECLARE @SupplierUpdates TABLE (
    SupplierID_NK INT,
    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(100),
    ContactTitle NVARCHAR(100),
    Address NVARCHAR(255),
    City NVARCHAR(100),
    Region NVARCHAR(100),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(100),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50),
    HomePage NVARCHAR(255),
    ActionTaken NVARCHAR(10)
);

MERGE DimSuppliers AS DST
USING dbo.Suppliers_Staging AS SRC
    ON SRC.SupplierID = DST.SupplierID_NK
WHEN NOT MATCHED BY TARGET THEN
    INSERT (SupplierID_NK, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
    VALUES (SRC.SupplierID, SRC.CompanyName, SRC.ContactName, SRC.ContactTitle, SRC.Address, SRC.City, SRC.Region, SRC.PostalCode, SRC.Country, SRC.Phone, SRC.Fax, SRC.HomePage)
WHEN MATCHED AND (
       ISNULL(DST.CompanyName, '') <> ISNULL(SRC.CompanyName, '') OR
       ISNULL(DST.ContactName, '') <> ISNULL(SRC.ContactName, '') OR
       ISNULL(DST.ContactTitle, '') <> ISNULL(SRC.ContactTitle, '') OR
       ISNULL(DST.Address, '') <> ISNULL(SRC.Address, '') OR
       ISNULL(DST.City, '') <> ISNULL(SRC.City, '') OR
       ISNULL(DST.Region, '') <> ISNULL(SRC.Region, '') OR
       ISNULL(DST.PostalCode, '') <> ISNULL(SRC.PostalCode, '') OR
       ISNULL(DST.Country, '') <> ISNULL(SRC.Country, '') OR
       ISNULL(DST.Phone, '') <> ISNULL(SRC.Phone, '') OR
       ISNULL(DST.Fax, '') <> ISNULL(SRC.Fax, '') OR
       ISNULL(DST.HomePage, '') <> ISNULL(SRC.HomePage, '')
   )
THEN
    UPDATE
    SET
        DST.CompanyName = SRC.CompanyName,
        DST.ContactName = SRC.ContactName,
        DST.ContactTitle = SRC.ContactTitle,
        DST.Address = SRC.Address,
        DST.City = SRC.City,
        DST.Region = SRC.Region,
        DST.PostalCode = SRC.PostalCode,
        DST.Country = SRC.Country,
        DST.Phone = SRC.Phone,
        DST.Fax = SRC.Fax,
        DST.HomePage = SRC.HomePage
OUTPUT
    $action AS ActionTaken,
    SRC.SupplierID AS SupplierID_NK, -- Ensure correct mapping
    SRC.CompanyName,
    SRC.ContactName,
    SRC.ContactTitle,
    SRC.Address,
    SRC.City,
    SRC.Region,
    SRC.PostalCode,
    SRC.Country,
    SRC.Phone,
    SRC.Fax,
    SRC.HomePage
INTO @SupplierUpdates;

INSERT INTO DimSuppliers_SCD4_History
(SupplierID_NK, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage, ArchiveDate)
SELECT
    SupplierID_NK, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage, @Today
FROM DimSuppliers
WHERE EXISTS (
    SELECT 1
    FROM @SupplierUpdates U
    WHERE U.SupplierID_NK = DimSuppliers.SupplierID_NK -- Corrected column reference
      AND U.ActionTaken = 'UPDATE'
);

SELECT * FROM DimSuppliers;
SELECT * FROM DimSuppliers_SCD4_History;

---------------------------------------------------------------------------
-- DimTerritories SCD 3
---------------------------------------------------------------------------
IF OBJECT_ID('dbo.DimTerritories_SCD3', 'U') IS NOT NULL
    DROP TABLE dbo.DimTerritories_SCD3;

CREATE TABLE dbo.DimTerritories_SCD3 (
    TerritoriesID_sk_pk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    TerritoryID_nk INT NOT NULL,
    TerritoryDescription NVARCHAR(50) NULL,
    TerritoryDescription_Prev1 NVARCHAR(50) NULL,
    TerritoryDescription_Prev1_ValidTo DATE NULL,
    TerritoryCode NVARCHAR(20) NULL,
    TerritoryCode_Prev1 NVARCHAR(20) NULL,
    TerritoryCode_Prev1_ValidTo DATE NULL
);

MERGE INTO dbo.DimTerritories_SCD3 AS DST
USING dbo.Territories_Staging AS SRC
    ON SRC.TerritoryID = DST.TerritoryID_nk
WHEN NOT MATCHED THEN
    INSERT (TerritoryID_nk, TerritoryDescription, TerritoryCode)
    VALUES (SRC.TerritoryID, SRC.TerritoryDescription, SRC.TerritoryCode)
WHEN MATCHED AND (
       COALESCE(DST.TerritoryDescription, '') <> COALESCE(SRC.TerritoryDescription, '')
    OR COALESCE(DST.TerritoryCode, '') <> COALESCE(SRC.TerritoryCode, '')
)
THEN
    UPDATE
    SET
        TerritoryDescription_Prev1 = DST.TerritoryDescription,
        TerritoryDescription = SRC.TerritoryDescription,
        TerritoryDescription_Prev1_ValidTo = @Yesterday,
        TerritoryCode_Prev1 = DST.TerritoryCode,
        TerritoryCode = SRC.TerritoryCode,
        TerritoryCode_Prev1_ValidTo = @Yesterday;

SELECT * FROM dbo.DimTerritories_SCD3;

---------------------------------------------------------------------------
-- FACT TABLE SNAPSHOT
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
    LEFT JOIN dbo.DimCustomers     AS c   ON stg.CustomerID  = c.CustomerID_nk
    LEFT JOIN dbo.DimEmployees     AS e   ON stg.EmployeeID  = e.EmployeeID_nk
    LEFT JOIN dbo.DimShippers      AS shp ON stg.ShipVia     = shp.ShipperID_NK
    LEFT JOIN dbo.DimTerritories_SCD3 AS ter ON stg.TerritoryID = ter.TerritoryID_nk
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

SELECT * FROM dbo.FactOrders;
