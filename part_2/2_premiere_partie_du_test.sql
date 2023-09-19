SELECT `date`, SUM(prod_price * prod_qty) AS ventes
FROM TRANSACTION
WHERE `date` BETWEEN DATE('01-01-2019') AND DATE('31-12-2019')
GROUP BY `date`
ORDER BY `date`

-- column `date` is between 2 `` in order to avoid confusions with the SQL key word DATE. We could also use TRANSACTION.date.