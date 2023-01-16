WITH

  users_sous_secteur AS (
  SELECT
    LOWER(TRIM(sous_secetur.user)) AS user,
    sous_secetur.id_fournisseur,
    sous_secetur.id_marque,
    sous_secetur.id_secteur,
    sous_secetur.id_sous_secteur
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_user_sous_secteur` AS sous_secetur ),
    
  insights_stock_last_day AS(
  SELECT
    DISTINCT stocks.*,
    ref_sous_secteur.user,
    'externe' AS user_type
  FROM (
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day`) stocks
  JOIN
    users_sous_secteur ref_sous_secteur
  ON
    stocks.id_fournisseur = ref_sous_secteur.id_fournisseur
    AND stocks.id_marque = ref_sous_secteur.id_marque
    AND stocks.id_secteur = ref_sous_secteur.id_secteur
    AND stocks.id_sous_secteur= ref_sous_secteur.id_sous_secteur)
SELECT
  DISTINCT insights_stock_last_day.*,
  CURRENT_DATE() AS update_date
FROM
  insights_stock_last_day
LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` AS user_order_insertion
ON
  insights_stock_last_day.user = LOWER(TRIM(user_order_insertion.user))
WHERE
  user_order_insertion.insertion_date < CURRENT_DATE()