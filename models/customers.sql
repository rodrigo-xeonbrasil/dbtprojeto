-- customer model
WITH MARKUP AS (
SELECT *,
    FIRST_VALUE(customer_id)
    OVER (
        PARTITION BY company_name, contact_name
        ORDER BY company_name
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS result
FROM {{ source('sources', 'customers') }}
), REMOVED AS (SELECT DISTINCT RESULT FROM MARKUP),
FINAL AS (SELECT * FROM {{ source('sources', 'customers') }} WHERE CUSTOMER_ID IN (SELECT RESULT FROM REMOVED))
SELECT * FROM FINAL