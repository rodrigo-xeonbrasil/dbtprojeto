SELECT 
    od.order_id, 
    od.product_id, 
    od.quantity,  
    pr.product_name, 
    pr.supplier_id, 
    pr.category_id,
    od.unit_price * od.quantity AS total,
    (od.unit_price * od.quantity) - od.unit_price * od.quantity AS discount
FROM {{ source('sources', 'order_details') }} od
LEFT JOIN {{ source('sources', 'products') }} pr 
    ON od.product_id = pr.product_id