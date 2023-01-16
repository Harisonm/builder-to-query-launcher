SELECT
  *
FROM (
  SELECT
    CURRENT_DATE() AS insertion_date,
    *,
    '' AS user,
    'interne' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_agg`
  WHERE
    date='{date_begin}')
UNION ALL (
  SELECT
    CURRENT_DATE() AS insertion_date,
    ecom.*,
    ref.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_agg` ecom
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_marque` ref
  ON
    ref.id_marque = ecom.id_marque
  WHERE
    date='{date_begin}')
UNION ALL (
  SELECT
    CURRENT_DATE() AS insertion_date,
    ecommerce_agg.*,
    ref_cat.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_agg` AS ecommerce_agg
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_category` AS ref_cat
  ON
    ref_cat.category_1 = ecommerce_agg.category_1
  WHERE
    date='{date_begin}')