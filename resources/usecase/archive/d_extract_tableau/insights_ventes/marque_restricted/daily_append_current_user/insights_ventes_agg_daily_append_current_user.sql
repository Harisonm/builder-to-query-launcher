WITH
  restricted_marque_secteur AS (
  SELECT
    LOWER(TRIM(marque.user)) AS user,
    marque.id_fournisseur,
    marque.id_marque
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_stock_vente_fournisseur_marque` AS marque
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` AS secteur
  ON
    LOWER(TRIM(marque.user)) = LOWER(TRIM(secteur.user))
  WHERE
    LOWER(TRIM(secteur.user)) IS NULL),
  insights_ventes_agg AS (
  SELECT
    DISTINCT ventes.*,
    ref_marque.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN
    restricted_marque_secteur ref_marque
  ON
    ventes.id_fournisseur = ref_marque.id_fournisseur
    AND ventes.id_marque = ref_marque.id_marque
  JOIN (
    SELECT
      user,
      MAX(date) AS last_update
    FROM
      `plateforme-fournisseurs-prod.d_extract_tableau.insights_ventes_agg_production`
    GROUP BY
      1 ) current_users
  ON
    LOWER(TRIM(ref_marque.user)) = LOWER(TRIM(current_users.user))
  WHERE
    ventes.date > current_users.last_update),
  p3_ventes_restricted_marque AS (
  SELECT
    DISTINCT insights_ventes_agg .*,
    DATE(CURRENT_DATE()) AS update_date
  FROM
    insights_ventes_agg
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` AS user_order_insertion
  ON
    LOWER(TRIM(insights_ventes_agg.user)) = LOWER(TRIM(user_order_insertion.user))
  WHERE
    user_order_insertion.insertion_date <= CURRENT_DATE())
SELECT
  *
FROM
  p3_ventes_restricted_marque