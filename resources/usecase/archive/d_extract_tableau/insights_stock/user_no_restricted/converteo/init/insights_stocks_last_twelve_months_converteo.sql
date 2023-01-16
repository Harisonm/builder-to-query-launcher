WITH converteo_users AS (SELECT
  stocks.*,
  conv_users.user,
  DATE(CURRENT_DATE()) AS update_date
FROM
  `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months` stocks
  JOIN (SELECT
    DISTINCT LOWER(TRIM(user)) AS user,
    id_fournisseur
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` AS ref_fournisseur
  WHERE
    user LIKE '%@converteo.com') AS conv_users
ON
  stocks.id_fournisseur = conv_users.id_fournisseur)
SELECT * FROM converteo_users