WITH
  insights_stock_last_day AS (
  SELECT
    DISTINCT stocks.*,
    LOWER(TRIM(ref_secteur.user)) as user,
    'externe' AS user_type
  FROM
    (select * from `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day`) stocks
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` ref_secteur
  ON
    stocks.id_fournisseur = ref_secteur.id_fournisseur
    AND stocks.id_marque = ref_secteur.id_marque
    AND stocks.id_secteur = ref_secteur.id_secteur)
SELECT
  DISTINCT insights_stock_last_day.*,
  CURRENT_DATE() as update_date
FROM
  insights_stock_last_day
LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` AS user_order_insertion
ON
  LOWER(TRIM(user_order_insertion.user))=LOWER(TRIM(insights_stock_last_day.user))
  where user_order_insertion.insertion_date < CURRENT_DATE()