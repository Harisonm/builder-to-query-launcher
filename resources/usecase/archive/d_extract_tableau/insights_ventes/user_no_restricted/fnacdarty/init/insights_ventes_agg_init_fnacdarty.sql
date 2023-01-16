WITH fnacdarty_data_user AS (SELECT
  ventes.*,
  fnac_darty_users.user,
  'externe' AS user_type,
  DATE(CURRENT_DATE()) AS update_date
FROM
  `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN (SELECT
    DISTINCT TRIM(user) AS user,
    id_fournisseur
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` AS ref_fournisseur
  WHERE
    user LIKE '%@fnacdarty.com') AS fnac_darty_users
ON
  ventes.id_fournisseur = fnac_darty_users.id_fournisseur)
SELECT * FROM fnacdarty_data_user