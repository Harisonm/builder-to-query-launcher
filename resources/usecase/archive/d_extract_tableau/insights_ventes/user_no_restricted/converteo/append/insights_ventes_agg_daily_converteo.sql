WITH
  ventes_agg_converteo AS (
  SELECT
    ventes.*,
    conv_users.user,
    DATE(CURRENT_DATE()) AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN (
    SELECT
      DISTINCT LOWER(TRIM(user)) AS user,
      id_fournisseur
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` AS ref_fournisseur
    WHERE
      user LIKE '%@converteo.com') AS conv_users
  ON
    ventes.id_fournisseur = conv_users.id_fournisseur
  JOIN (
    SELECT
      user,
      MAX(date) AS last_update
    FROM
      `plateforme-fournisseurs-prod.d_extract_tableau.insights_ventes_agg_production`
    GROUP BY
      1 ) current_users
  USING
    (user)
  WHERE
    ventes.date > current_users.last_update)
SELECT
  *
FROM
  ventes_agg_converteo