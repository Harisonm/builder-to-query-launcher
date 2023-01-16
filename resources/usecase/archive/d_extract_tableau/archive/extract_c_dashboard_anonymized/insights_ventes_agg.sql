SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type
  FROM
    `plateforme-fournisseurs-prod.datamarts_fournisseurs.insights_ventes_agg` )
UNION ALL (
  SELECT
    ventes.*,
    ref.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.datamarts_fournisseurs.insights_ventes_agg` ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` ref
  ON
    ref.id_fournisseur = ventes.id_fournisseur )