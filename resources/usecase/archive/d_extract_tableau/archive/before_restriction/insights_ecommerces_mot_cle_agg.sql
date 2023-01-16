SELECT
  *
FROM (
  SELECT
    CURRENT_DATE() AS insertion_date,
    *,
    '' AS user,
    'interne' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg` )
UNION ALL (
  SELECT
    CURRENT_DATE() AS insertion_date,
    ecom.*,
    ref.user,
    'externe' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg` ecom
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_marque` ref
  ON
    ref.id_marque = ecom.id_marque )
UNION ALL (
  SELECT
    CURRENT_DATE() AS insertion_date,
    ecom.*,
    ref_cat.user,
    'externe' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg` ecom
JOIN
`plateforme-fournisseurs-prod.referential_table.referential_user_category` AS ref_cat
ON
ref_cat.category_1 = ecom.category_1)
