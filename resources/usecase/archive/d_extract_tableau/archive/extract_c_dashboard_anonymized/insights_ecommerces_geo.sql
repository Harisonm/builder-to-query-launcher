SELECT
  *
FROM (
  SELECT
    * EXCEPT (id_marque_unanonymised),
    '' AS user,
    'interne' AS user_type
  FROM
    `plateforme-fournisseurs-prod.datamarts_fournisseurs.insights_ecommerces_geo` )
UNION ALL (
  SELECT
    ecom.* EXCEPT (id_marque_unanonymised),
    ref.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.datamarts_fournisseurs.insights_ecommerces_geo` ecom
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_marque` ref
  ON
    ref.id_marque = ecom.id_marque_unanonymised)