SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients` )
UNION ALL (
  SELECT
    clients.*,
    ref.user,
    'externe' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients` clients
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_marque` ref
  ON
    ref.id_marque = clients.marque_full )
UNION ALL (
  SELECT
    clients.*,
    ref_cat.user,
    'externe' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients` AS clients
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_category` AS ref_cat
  ON
    ref_cat.category_1 = clients.famille)
