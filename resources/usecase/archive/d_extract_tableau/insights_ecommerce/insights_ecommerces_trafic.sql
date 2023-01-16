SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_trafic` )
UNION ALL (
  SELECT
    ecom.*,
    ref.user,
    'externe' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_trafic` ecom
JOIN
`plateforme-fournisseurs-prod.referential_table.referential_user_marque` AS ref
  ON
    ref.id_marque = ecom.id_marque )
UNION ALL (
  SELECT
    ecom.*,
    ref_cat.user,
    'externe' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_trafic` ecom
JOIN
`plateforme-fournisseurs-prod.referential_table.referential_user_category` AS ref_cat
ON
ref_cat.category_1 = ecom.category_1)
