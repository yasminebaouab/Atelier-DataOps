SELECT
  DATE(invoice_date) AS sale_date,
  SUM(quantity * unit_price) AS daily_revenue
FROM {{ ref('stg_sales') }}
GROUP BY 1
ORDER BY 1
