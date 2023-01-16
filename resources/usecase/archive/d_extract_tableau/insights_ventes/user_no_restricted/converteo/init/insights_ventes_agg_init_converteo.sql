WITH converteo_users AS (SELECT
  ventes.*,
  conv_users.user,
  DATE(CURRENT_DATE()) AS update_date
FROM
  `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN (SELECT
    DISTINCT LOWER(TRIM(user)) AS user,
    id_fournisseur
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` AS ref_fournisseur
  WHERE
    user LIKE '%@converteo.com') AS conv_users
ON
  ventes.id_fournisseur = conv_users.id_fournisseur)
SELECT * FROM converteo_users