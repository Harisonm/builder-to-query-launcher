WITH
  stocks_agg AS (
  SELECT
    DISTINCT stocks.*,
    LOWER(TRIM(ref_sous_secteur.user)) AS user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day` stocks
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_sous_secteur` ref_sous_secteur
  ON
    stocks.id_fournisseur = ref_sous_secteur.id_fournisseur
    AND stocks.id_marque = ref_sous_secteur.id_marque
    AND stocks.id_secteur = ref_sous_secteur.id_secteur
    AND stocks.id_sous_secteur= ref_sous_secteur.id_sous_secteur
  JOIN (
    SELECT
      user,
      MAX(import_date) AS last_update
    FROM
      `plateforme-fournisseurs-prod.d_extract_tableau.insights_stocks_last_day`
    GROUP BY
      1 ) current_users
  USING
    (user)
  WHERE
    stocks.import_date > current_users.last_update)
SELECT
  *
FROM
  stocks_agg