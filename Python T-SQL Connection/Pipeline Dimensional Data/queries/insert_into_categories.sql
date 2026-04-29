
-- ./insert_into_categories.sql

-- Insert data into table: categories
INSERT INTO ORDER_DDS.schema.categories
   ([CategoryID], [CategoryName], [Description])
VALUES
    (?, ?, ?);
