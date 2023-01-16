SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type,
    DATE(CURRENT_DATETIME()) AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_day`)