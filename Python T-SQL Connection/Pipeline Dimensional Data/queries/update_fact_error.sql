-- ===========================
-- 1. Parameter Definitions
-- ===========================
DECLARE @database_name NVARCHAR(100) = 'ORDER_DDS';
DECLARE @schema_name NVARCHAR(100) = 'dbo';
DECLARE @fact_error_table_name NVARCHAR(100) = 'FactError';
DECLARE @start_date DATETIME = '2024-01-01';
DECLARE @end_date DATETIME = '2024-12-31';

-- ===========================
-- 2. Context Switching
-- ===========================
DECLARE @use_db NVARCHAR(200) = N'USE [' + @database_name + N'];';
EXEC sp_executesql @use_db;


-- ===========================
-- 3. Ingestion Logic
-- ===========================
PRINT 'Starting ingestion of faulty rows into the fact_error table...';

INSERT INTO [dbo].[FactError] (
    staging_raw_order_id,
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
    ShipCountry,
    ErrorDescription,
    IngestionDate
)
SELECT
    stg.staging_raw_order_id,
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
    stg.ShipCountry,
    CASE
        WHEN DimCust.CustomersID_sk_pk IS NULL THEN 'Missing Customer_sk_fk'
        WHEN DimEmp.EmployeeID_sk_pk IS NULL THEN 'Missing Employee_sk_fk'
        WHEN DimShippers.ShippersID_sk_pk IS NULL THEN 'Missing Shippers_sk_fk'
        WHEN DimTerr.TerritoriesID_sk_pk IS NULL THEN 'Missing Territories_sk_fk'
        ELSE 'Unknown Error'
    END AS ErrorDescription,
    GETDATE() AS IngestionDate
FROM dbo.Orders_Staging AS stg
LEFT JOIN dbo.DimCustomers AS DimCust ON stg.CustomerID = DimCust.CustomerID_nk
LEFT JOIN dbo.DimEmployees AS DimEmp ON stg.EmployeeID = DimEmp.EmployeeID_nk
LEFT JOIN dbo.DimShippers AS DimShippers ON stg.ShipVia = DimShippers.ShipperID_nk
LEFT JOIN dbo.DimTerritories AS DimTerr ON stg.TerritoryID = DimTerr.TerritoryID_nk
WHERE
    (
        DimCust.CustomersID_sk_pk IS NULL
        OR DimEmp.EmployeeID_sk_pk IS NULL
        OR DimShippers.ShippersID_sk_pk IS NULL
        OR DimTerr.TerritoriesID_sk_pk IS NULL
    )
    AND stg.OrderDate BETWEEN @start_date AND @end_date;

PRINT 'Ingestion of faulty rows completed successfully.';
