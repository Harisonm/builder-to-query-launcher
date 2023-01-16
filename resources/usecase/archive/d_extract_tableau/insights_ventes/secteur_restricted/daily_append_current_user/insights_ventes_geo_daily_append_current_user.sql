WITH
  ventes_geo AS (
  SELECT
    DISTINCT ventes.*,
    LOWER(TRIM(ref_secteur.user)) AS user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_geo` ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` ref_secteur
  ON
    ventes.id_fournisseur = ref_secteur.id_fournisseur
    AND ventes.id_marque = ref_secteur.id_marque
    AND ventes.id_secteur = ref_secteur.id_secteur
  JOIN (
    SELECT
      user,
      MAX(date) AS last_update
    FROM
      `plateforme-fournisseurs-prod.d_extract_tableau.insights_ventes_geo_production`
    GROUP BY
      1 ) current_users
  USING
    (user)
  WHERE
    ventes.date > current_users.last_update),
  p3_ventes_restricted_secteur AS (
  SELECT
    DISTINCT ventes_geo.*,
    DATE(CURRENT_DATE()) AS update_date
  FROM
    ventes_geo
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` AS user_order_insertion
  ON
    LOWER(TRIM(user_order_insertion.user))=LOWER(TRIM(ventes_geo.user))
    WHERE
  user_order_insertion.insertion_date <= CURRENT_DATE() )
SELECT
  *
FROM
  p3_ventes_restricted_secteur