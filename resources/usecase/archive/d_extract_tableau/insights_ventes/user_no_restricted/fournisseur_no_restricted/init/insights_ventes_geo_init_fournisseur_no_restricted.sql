WITH
  fournisseur_restricted AS (
  SELECT
    DISTINCT LOWER(TRIM(user)) AS user,
    id_fournisseur
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` AS ref_fournisseur
  LEFT JOIN (
    SELECT
      DISTINCT LOWER(TRIM(user)) AS user,
      id_fournisseur
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_secteur`) ref_secteur
  USING
    (user,
      id_fournisseur)
  WHERE
    ref_secteur.user IS NULL),
  marque_restricted AS (
  SELECT
    DISTINCT fournisseur_restricted.*
  FROM
    fournisseur_restricted
  LEFT JOIN (
    SELECT
      DISTINCT LOWER(TRIM(ref_marque.user)) AS user,
      ref_marque.id_fournisseur,
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_stock_vente_fournisseur_marque` AS ref_marque) AS ref_fournisseur_marque_restricted
  USING
    (user,
      id_fournisseur)
  WHERE
    ref_fournisseur_marque_restricted.user IS NULL),
  fournisseur_converteo_fnacdarty_restricted AS (
  SELECT
    *
  FROM
    marque_restricted
  WHERE
    LOWER(TRIM(user)) NOT LIKE '%@fnacdarty.com'
    AND LOWER(TRIM(user)) NOT LIKE '%@converteo.com'),
  insights_ventes_geo_fournisseur_converteo_fnacdarty_restricted AS (
  SELECT
    ventes.*,
    fournisseur.user
  FROM
    fournisseur_converteo_fnacdarty_restricted AS fournisseur
  JOIN
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_geo` AS ventes
  ON
    ventes.id_fournisseur = fournisseur.id_fournisseur )
SELECT
  DISTINCT *
FROM
  insights_ventes_geo_fournisseur_converteo_fnacdarty_restricted