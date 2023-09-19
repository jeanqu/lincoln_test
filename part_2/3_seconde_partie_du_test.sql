-- For lisibility, we create first a block 'WITH', but we could do it directly
WITH interesting_transactions AS (
    SELECT 
        transac.client_id, 
        transac.prod_price * transac.prod_qty AS ventes, 
        prod.product_type
    FROM TRANSACTION AS transac
    INNER JOIN PRODUCT_NOMENCLATURE AS prod
        ON transac.prod_id = prod.product_id
        AND prod.product_type IN ('DECO', 'MEUBLE') -- Should not be needed but just in case
    WHERE transac.date BETWEEN DATE('01-01-2020') AND DATE('31-12-2020'))

SELECT 
    client_id,
    SUM(IF(product_type = 'MEUBLE', ventes, 0)) AS ventes_meuble,
    SUM(IF(product_type = 'DECO', ventes, 0)) AS ventes_deco
FROM interesting_transactions
GROUP BY client_id
