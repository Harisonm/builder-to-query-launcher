SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_geo` )
UNION ALL (
  SELECT
    ecom.*,
    ref.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_geo` ecom
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_marque` ref
  ON
    ref.id_marque = ecom.id_marque )