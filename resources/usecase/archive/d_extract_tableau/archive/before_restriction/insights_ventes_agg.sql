WITH fournisseur_restricted AS (
  SELECT
    ref_fournisseur.*,
    'externe' AS user_type,
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` AS ref_fournisseur
  LEFT JOIN (
    SELECT
      DISTINCT user,
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
    fournisseur_restricted.*,
    ref_frn_marque.id_marque
  FROM
    fournisseur_restricted
   LEFT JOIN (
    SELECT
      DISTINCT ref_marque.user,
      ref_marque.id_fournisseur,
      ref_marque.id_marque
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_stock_vente_fournisseur_marque` AS ref_marque) AS ref_fournisseur_marque_restricted
  USING
    (user,
      id_fournisseur)

  LEFT JOIN `plateforme-fournisseurs-prod.referential_table.referential_fournisseur_marque` AS ref_frn_marque 
  ON
    fournisseur_restricted.id_fournisseur=ref_frn_marque.id_fournisseur
  WHERE
    ref_fournisseur_marque_restricted.user IS NULL),
  p3_restriction_user AS (
  SELECT
    ventes.*,
    ref_secteur.user,
    'externe' AS user_type,
    CURRENT_DATETIME() AS update_date
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` ref_secteur
  ON
    ventes.id_fournisseur = ref_secteur.id_fournisseur
    AND ventes.id_marque = ref_secteur.id_marque
    AND ventes.id_secteur = ref_secteur.id_secteur
  UNION ALL (
    SELECT
      ventes.*,
      marque_restricted.user,
      'externe' AS user_type,
      CURRENT_DATETIME() AS update_date
    FROM
      `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
    JOIN
      marque_restricted
    ON
      marque_restricted.id_fournisseur=ventes.id_fournisseur
      AND marque_restricted.id_marque=ventes.id_marque )
  UNION ALL (
    SELECT
      ventes.*,
      ref_stk_vt_frn_marque.user,
      'externe' AS user_type,
      CURRENT_DATETIME() AS update_date
    FROM
      `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` AS ventes
    JOIN
      `plateforme-fournisseurs-prod.referential_table.referential_stock_vente_fournisseur_marque` AS ref_stk_vt_frn_marque
    ON
      ventes.id_fournisseur = ref_stk_vt_frn_marque.id_fournisseur
      AND ventes.id_marque = ref_stk_vt_frn_marque.id_marque )
  UNION ALL
  SELECT
    *
  FROM (
    SELECT
      *,
      '' AS user,
      'interne' AS user_type,
      CURRENT_DATETIME() AS update_date
    FROM
      `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ) )
SELECT
  *
FROM
  p3_restriction_user
