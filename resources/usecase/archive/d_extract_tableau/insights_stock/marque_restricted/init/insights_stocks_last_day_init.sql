WITH restricted_marque_secteur AS (
    SELECT
          LOWER(TRIM(marque.user)) AS user,
         marque.id_fournisseur,
         marque.id_marque
    FROM
        `plateforme-fournisseurs-prod.referential_table.referential_stock_vente_fournisseur_marque` AS marque
        LEFT JOIN `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` AS secteur ON LOWER(TRIM(marque.user)) = LOWER(TRIM(secteur.user)) 
    WHERE
        LOWER(TRIM(secteur.user)) IS NULL), 
  insights_stock_last_day AS(
  SELECT
    DISTINCT stocks.*,
    ref_marque.user,
    'externe' AS user_type
  FROM
    (SELECT * FROM `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day`) stocks
  JOIN
    restricted_marque_secteur ref_marque
  ON
    stocks.id_fournisseur = ref_marque.id_fournisseur
    AND stocks.id_marque = ref_marque.id_marque)

SELECT
  DISTINCT insights_stock_last_day.*,
  CURRENT_DATE() AS update_date
FROM
  insights_stock_last_day
LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion`  AS user_order_insertion
  ON LOWER(TRIM(insights_stock_last_day.user)) = LOWER(TRIM(user_order_insertion.user)) 
  WHERE user_order_insertion.insertion_date < CURRENT_DATE()