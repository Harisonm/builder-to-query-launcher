SELECT
    clients.*,
    ref.user,
    'externe' AS user_type
FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients` clients
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
    ref.id_marque = clients.marque_full
