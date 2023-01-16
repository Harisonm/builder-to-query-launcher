WITH
  stocks_agg AS (
  SELECT
    DISTINCT stocks.*,
    LOWER(TRIM(ref_secteur.user)) AS user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months` stocks
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` ref_secteur
  ON
    stocks.id_fournisseur = ref_secteur.id_fournisseur
    AND stocks.id_marque = ref_secteur.id_marque
    AND stocks.id_secteur = ref_secteur.id_secteur
  JOIN (
    SELECT
      user,
      MAX(import_date) AS last_update
    FROM
      `plateforme-fournisseurs-prod.d_extract_tableau.insights_stocks_last_twelve_months`
    GROUP BY
      1 ) current_users
  USING
    (user)
  WHERE
    stocks.import_date > current_users.last_update),
  p3_stocks_restricted_secteur AS (
  SELECT
    DISTINCT stocks_agg.*,
    DATE(CURRENT_DATE()) AS update_date
  FROM
    stocks_agg
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` AS user_order_insertion
  ON
    LOWER(TRIM(user_order_insertion.user))=LOWER(TRIM(stocks_agg.user))
    WHERE
  user_order_insertion.insertion_date <= CURRENT_DATE() )
SELECT
  *
FROM
  p3_stocks_restricted_secteur