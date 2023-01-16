WITH
  insights_stock_last_12_months AS (
  SELECT
    DISTINCT stocks.*,
    LOWER(TRIM(ref_sous_secteur.user)) AS user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months` stocks
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_sous_secteur` ref_sous_secteur
  ON
    stocks.id_fournisseur = ref_sous_secteur.id_fournisseur
    AND stocks.id_marque = ref_sous_secteur.id_marque
    AND stocks.id_secteur = ref_sous_secteur.id_secteur
    AND stocks.id_sous_secteur = ref_sous_secteur.id_sous_secteur
  ),
new_users AS (
  SELECT * FROM `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` users 
  CROSS JOIN  (SELECT MAX(update_date) last_update_date FROM `plateforme-fournisseurs-prod.d_extract_tableau.insights_stocks_last_twelve_months` ) last_update
  WHERE users.insertion_date > last_update_date and users.insertion_date <= CURRENT_DATE()
  )

SELECT
  DISTINCT insights_stock_last_12_months.*,
  CURRENT_DATE() as update_date
FROM
  insights_stock_last_12_months JOIN new_users ON LOWER(TRIM(new_users.user))=LOWER(TRIM(insights_stock_last_12_months.user))