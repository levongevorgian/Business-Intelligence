
-- ./insert_into_orderdetails.sql

-- Insert data into table: orderdetails
INSERT INTO ORDER_DDS.schema.orderdetails
   ([OrderID], [ProductID], [UnitPrice], [Quantity], [Discount])
VALUES
    (?, ?, ?, ?, ?);
