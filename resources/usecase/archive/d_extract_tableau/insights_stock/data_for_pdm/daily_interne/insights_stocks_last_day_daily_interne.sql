SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type,
    DATE(CURRENT_DATETIME()) AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day`
  WHERE
    import_date > (
    SELECT
      MAX(import_date)
    FROM
      `plateforme-fournisseurs-prod.d_extract_tableau.insights_stocks_last_day` ))