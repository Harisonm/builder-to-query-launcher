SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type,
    DATE(CURRENT_DATETIME()) AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg`
  WHERE
    date > (
    SELECT
      MAX(date)
    FROM
      `plateforme-fournisseurs-prod.d_extract_tableau.insights_ventes_agg_production` ))