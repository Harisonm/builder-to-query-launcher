WITH
  ventes_agg AS (
  SELECT
    DISTINCT ventes.*,
    LOWER(TRIM(ref_sous_secteur.user)) AS user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_sous_secteur` ref_sous_secteur
  ON
    ventes.id_fournisseur = ref_sous_secteur.id_fournisseur
    AND ventes.id_marque = ref_sous_secteur.id_marque
    AND ventes.id_secteur = ref_sous_secteur.id_secteur
    AND ventes.id_sous_secteur= ref_sous_secteur.id_sous_secteur
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
  ventes_agg