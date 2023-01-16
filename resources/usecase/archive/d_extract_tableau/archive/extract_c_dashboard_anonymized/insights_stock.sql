SELECT
  *
FROM (
  SELECT
    *,
    '' AS user,
    'interne' AS user_type
  FROM
    `plateforme-fournisseurs-prod.datamarts_fournisseurs.insights_stock` )
UNION ALL (
  SELECT
    stock.*,
    ref.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.datamarts_fournisseurs.insights_stock` stock
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` ref
  ON
    ref.id_fournisseur = stock.id_fournisseur )