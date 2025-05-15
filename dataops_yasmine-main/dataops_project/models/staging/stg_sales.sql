SELECT
  CAST("InvoiceNo" AS TEXT) AS invoice_no,
  CAST("StockCode" AS TEXT) AS stock_code,
  INITCAP(TRIM("Description")) AS description,
  CAST("Quantity" AS INTEGER) AS quantity,
  CAST("InvoiceDate" AS TIMESTAMP) AS invoice_date,
  CAST("UnitPrice" AS NUMERIC(10,2)) AS unit_price,
  CAST("CustomerID" AS INTEGER) AS customer_id,
  INITCAP(TRIM("Country")) AS country
FROM {{ source('raw', 'raw_sales') }}
