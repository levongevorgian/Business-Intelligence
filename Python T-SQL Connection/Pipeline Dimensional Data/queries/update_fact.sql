-- ================================================================
-- Parametrized SQL Script: update_fact.sql
-- Purpose: Ingest data into the fact table from staging raw and dimension tables.
-- Supports both MERGE-based and INSERT-based ingestion methods.
-- ================================================================

-- ===========================
-- 1. Parameter Definitions
-- ===========================
-- Replace the placeholder values with actual parameters before executing the script.

DECLARE @database_name NVARCHAR(100) = 'ORDER_DDS';       -- Target Database Name
DECLARE @schema_name NVARCHAR(100) = 'dbo';               -- Target Schema Name
DECLARE @fact_table_name NVARCHAR(100) = 'FactOrders';    -- Fact Table Name
DECLARE @ingestion_type NVARCHAR(10) = 'MERGE';           -- Ingestion Type: 'MERGE' or 'INSERT'
DECLARE @start_date DATETIME = '2024-01-01';              -- Start Date for Ingestion
DECLARE @end_date DATETIME = '2024-12-31';                -- End Date for Ingestion

-- ===========================
-- 2. Context Switching
-- ===========================
-- Switch to the target database.

DECLARE @use_db NVARCHAR(200) = N'USE [' + @database_name + N'];';
EXEC sp_executesql @use_db;

-- ===========================
-- 3. Ingestion Logic
-- ===========================
-- Perform ingestion based on the specified ingestion type.

IF UPPER(@ingestion_type) = 'MERGE'
BEGIN
    PRINT 'Starting MERGE-based ingestion...';

    MERGE INTO [@schema_name].[@fact_table_name] AS Target
    USING (
        SELECT
            stg.OrderID AS OrderID_nk,
            stg.OrderDate,
            stg.RequiredDate,
            stg.ShippedDate,
            DimCust.CustomersID_sk_pk AS Customer_sk_fk,
            DimEmp.EmployeeID_sk_pk AS Employee_sk_fk,
            DimShippers.ShippersID_sk_pk AS ShipVia,
            DimTerr.TerritoriesID_sk_pk AS Territory_sk_fk,
            stg.Freight,
            stg.ShipName,
            stg.ShipAddress,
            stg.ShipCity,
            stg.ShipRegion,
            stg.ShipPostalCode,
            stg.ShipCountry
        FROM dbo.Orders_Staging AS stg
        LEFT JOIN dbo.DimCustomers AS DimCust ON stg.CustomerID = DimCust.CustomerID_nk
        LEFT JOIN dbo.DimEmployees AS DimEmp ON stg.EmployeeID = DimEmp.EmployeeID_nk
        LEFT JOIN dbo.DimShippers AS DimShippers ON stg.ShipVia = DimShippers.ShipperID_nk
        LEFT JOIN dbo.DimTerritories AS DimTerr ON stg.TerritoryID = DimTerr.TerritoryID_nk
        -- Include additional joins with Dim_SOR if necessary
    ) AS Source
    ON Target.OrderID_nk = Source.OrderID_nk
    WHEN MATCHED AND (
           ISNULL(Target.Customer_sk_fk, 0) <> ISNULL(Source.Customer_sk_fk, 0)
        OR ISNULL(Target.Employee_sk_fk, 0) <> ISNULL(Source.Employee_sk_fk, 0)
        OR ISNULL(Target.ShipVia, 0) <> ISNULL(Source.ShipVia, 0)
        OR ISNULL(Target.Territory_sk_fk, 0) <> ISNULL(Source.Territory_sk_fk, 0)
        OR ISNULL(Target.OrderDate, '1900-01-01') <> ISNULL(Source.OrderDate, '1900-01-01')
        OR ISNULL(Target.RequiredDate, '1900-01-01') <> ISNULL(Source.RequiredDate, '1900-01-01')
        OR ISNULL(Target.ShippedDate, '1900-01-01') <> ISNULL(Source.ShippedDate, '1900-01-01')
        OR ISNULL(Target.Freight, 0) <> ISNULL(Source.Freight, 0)
        OR ISNULL(Target.ShipName, '') <> ISNULL(Source.ShipName, '')
        OR ISNULL(Target.ShipAddress, '') <> ISNULL(Source.ShipAddress, '')
        OR ISNULL(Target.ShipCity, '') <> ISNULL(Source.ShipCity, '')
        OR ISNULL(Target.ShipRegion, '') <> ISNULL(Source.ShipRegion, '')
        OR ISNULL(Target.ShipPostalCode, '') <> ISNULL(Source.ShipPostalCode, '')
        OR ISNULL(Target.ShipCountry, '') <> ISNULL(Source.ShipCountry, '')
    )
    THEN
        UPDATE SET
           Customer_sk_fk   = Source.Customer_sk_fk,
           Employee_sk_fk   = Source.Employee_sk_fk,
           ShipVia          = Source.ShipVia,
           Territory_sk_fk  = Source.Territory_sk_fk,
           OrderDate        = Source.OrderDate,
           RequiredDate     = Source.RequiredDate,
           ShippedDate      = Source.ShippedDate,
           Freight          = Source.Freight,
           ShipName         = Source.ShipName,
           ShipAddress      = Source.ShipAddress,
           ShipCity         = Source.ShipCity,
           ShipRegion       = Source.ShipRegion,
           ShipPostalCode   = Source.ShipPostalCode,
           ShipCountry      = Source.ShipCountry
    WHEN NOT MATCHED BY TARGET
    THEN
        INSERT (
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
            Source.OrderID_nk,
            Source.Customer_sk_fk,
            Source.Employee_sk_fk,
            Source.ShipVia,
            Source.Territory_sk_fk,
            Source.OrderDate,
            Source.RequiredDate,
            Source.ShippedDate,
            Source.Freight,
            Source.ShipName,
            Source.ShipAddress,
            Source.ShipCity,
            Source.ShipRegion,
            Source.ShipPostalCode,
            Source.ShipCountry
        )
    WHEN NOT MATCHED BY SOURCE
    THEN
        DELETE;

    PRINT 'MERGE-based ingestion completed successfully.';
END
ELSE IF UPPER(@ingestion_type) = 'INSERT'
BEGIN
    PRINT 'Starting INSERT-based ingestion...';

    INSERT INTO [@schema_name].[@fact_table_name] (
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
    SELECT
        stg.OrderID AS OrderID_nk,
        DimCust.CustomersID_sk_pk AS Customer_sk_fk,
        DimEmp.EmployeeID_sk_pk AS Employee_sk_fk,
        DimShippers.ShippersID_sk_pk AS ShipVia,
        DimTerr.TerritoriesID_sk_pk AS Territory_sk_fk,
        stg.OrderDate,
        stg.RequiredDate,
        stg.ShippedDate,
        stg.Freight,
        stg.ShipName,
        stg.ShipAddress,
        stg.ShipCity,
        stg.ShipRegion,
        stg.ShipPostalCode,
        stg.ShipCountry
    FROM dbo.Orders_Staging AS stg
    LEFT JOIN dbo.DimCustomers AS DimCust ON stg.CustomerID = DimCust.CustomerID_nk
    LEFT JOIN dbo.DimEmployees AS DimEmp ON stg.EmployeeID = DimEmp.EmployeeID_nk
    LEFT JOIN dbo.DimShippers AS DimShippers ON stg.ShipVia = DimShippers.ShipperID_nk
    LEFT JOIN dbo.DimTerritories AS DimTerr ON stg.TerritoryID = DimTerr.TerritoryID_nk
    WHERE stg.OrderDate BETWEEN @start_date AND @end_date;

    PRINT 'INSERT-based ingestion completed successfully.';
END
ELSE
BEGIN
    PRINT 'Invalid ingestion_type parameter. Use MERGE or INSERT.';
END
