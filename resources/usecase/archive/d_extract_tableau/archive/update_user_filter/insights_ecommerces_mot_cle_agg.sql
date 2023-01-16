SELECT
    ecom.*,
    ref.user,
    'externe' AS user_type
FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg` ecom
JOIN ((
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_marque_backup`
    UNION ALL
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_marque`) EXCEPT DISTINCT (
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_marque` INTERSECT DISTINCT
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_marque_backup`)) ref
ON
    ref.id_marque = ecom.id_marque
