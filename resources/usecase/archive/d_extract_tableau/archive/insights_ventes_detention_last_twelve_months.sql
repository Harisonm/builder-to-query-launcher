SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_detention_last_twelve_months` )
UNION ALL (
  SELECT
    ventes.*,
    ref.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_detention_last_twelve_months` ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` ref
  ON
    ref.id_fournisseur = ventes.id_fournisseur )