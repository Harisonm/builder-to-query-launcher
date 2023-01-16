WITH
  table_monitoring AS (
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.ventes' AS name
  FROM
    `plateforme-fournisseurs-prod.data.ventes`
  GROUP BY
    day
  UNION ALL
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.stock' AS name
  FROM
    `plateforme-fournisseurs-prod.data.stock`
  GROUP BY
    day
  UNION ALL
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.darty_achats' AS name
  FROM
    `plateforme-fournisseurs-prod.data.darty_achats`
  GROUP BY
    day
  UNION ALL
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.darty_search' AS name
  FROM
    `plateforme-fournisseurs-prod.data.darty_search`
  GROUP BY
    day
  UNION ALL
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.darty_trafic' AS name
  FROM
    `plateforme-fournisseurs-prod.data.darty_trafic`
  GROUP BY
    day
  UNION ALL
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.fnac_achats' AS name
  FROM
    `plateforme-fournisseurs-prod.data.fnac_achats`
  GROUP BY
    day
  UNION ALL
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.fnac_search' AS name
  FROM
    `plateforme-fournisseurs-prod.data.fnac_search`
  GROUP BY
    day
  UNION ALL
  SELECT
    day AS date,
    COUNT(day) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.fnac_trafic' AS name
  FROM
    `plateforme-fournisseurs-prod.data.fnac_trafic`
  GROUP BY
    day
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.datamart.insights_ventes' AS name
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_ventes`
  GROUP BY
    date
  UNION ALL
  SELECT
    import_date AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.datamart.insights_stock' AS name
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_stock`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    import_date AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.datamart.insights_profiling_clients' AS name
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.datamart.insights_ecommerces' AS name
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_ecommerces`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.datamart.insights_ecommerces_mot_cle' AS name
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_ecommerces_mot_cle`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_trafic' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_trafic`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_trafic_comex' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_trafic_comex`
  GROUP BY
    date
  UNION ALL
  SELECT
    import_date AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    import_date AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg_comex' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg_comex`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ventes_geo' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_geo`
  GROUP BY
    date
  UNION ALL
  SELECT
    date AS date,
    COUNT(date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.c_dashboard.insights_ventes_geo_comex' AS name
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_geo_comex`
  GROUP BY
    date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_darty_niv1__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_darty_niv1__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_darty_niv2__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_darty_niv2__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_darty_niv3__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_darty_niv3__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_darty_niv4__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_darty_niv4__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_fnac_niv1__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_fnac_niv1__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_fnac_niv2__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_fnac_niv2__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_fnac_niv3__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_fnac_niv3__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(import_date) AS date,
    COUNT(import_date) AS nbr_row_day,
    'plateforme-fournisseurs-prod.data.clients_fnac_niv4__all_versions' AS name
  FROM
    `plateforme-fournisseurs-prod.data.clients_fnac_niv4__all_versions`
  GROUP BY
    import_date
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_vente_product' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_vente_product'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_stock_magasin_geo' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_stock_magasin_geo'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_magasins_geo' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_magasins_geo'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_stock_product' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_stock_product'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_fournisseur' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_fournisseur'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_fournisseur_group' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_fournisseur_group'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_fournisseur_marque' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_fournisseur_marque'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_user_famille' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_user_famille'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_user_category' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_user_category'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_user_fournisseur'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur_backup' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_user_fournisseur_backup'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_user_marque' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_user_marque'
  UNION ALL
  SELECT
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS date,
    row_count AS nbr_row_day,
    'plateforme-fournisseurs-prod.referential_table.referential_user_marque_backup' AS name
  FROM
    `plateforme-fournisseurs-prod.referential_table.__TABLES__`
  WHERE
    table_id = 'referential_user_marque_backup'),
  p2_table_monitoring AS (
  SELECT
    date,
    nbr_row_day,
    SPLIT(name, '.')[
  OFFSET
    (0)] project_id,
    SPLIT(name, '.')[
  OFFSET
    (1)] dataset_id,
    SPLIT(name, '.')[
  OFFSET
    (2)] table_id
  FROM
    table_monitoring ),
  information_table AS (
  SELECT
    table_id,
    'referential_table' AS dataset_id,
    'plateforme-fournisseurs-prod' AS project_id,
    SUM(size_bytes)/(1024*1024*1024) AS size_table_gb
  FROM
    referential_table.__TABLES__
  GROUP BY
    table_id
  UNION ALL
  SELECT
    table_id,
    'datamart' AS dataset_id,
    'plateforme-fournisseurs-prod' AS project_id,
    ROUND(SUM(size_bytes)/(1024*1024*1024),1) AS size_table_gb
  FROM
    datamart.__TABLES__
  GROUP BY
    table_id
  UNION ALL
  SELECT
    table_id,
    'c_dashboard' AS dataset_id,
    'plateforme-fournisseurs-prod' AS project_id,
    ROUND(SUM(size_bytes)/(1024*1024*1024),1) AS size_table_gb
  FROM
    c_dashboard.__TABLES__
  GROUP BY
    table_id
  UNION ALL
  SELECT
    table_id,
    'data' AS dataset_id,
    'plateforme-fournisseurs-prod' AS project_id,
    ROUND(SUM(size_bytes)/(1024*1024*1024),1) AS size_table_gb
  FROM
    data.__TABLES__
  GROUP BY
    table_id)
SELECT
  p2_table_monitoring.*,
  size_table_gb
FROM
  p2_table_monitoring
LEFT JOIN
  information_table
ON
  p2_table_monitoring.table_id = information_table.table_id
  AND p2_table_monitoring.dataset_id = information_table.dataset_id