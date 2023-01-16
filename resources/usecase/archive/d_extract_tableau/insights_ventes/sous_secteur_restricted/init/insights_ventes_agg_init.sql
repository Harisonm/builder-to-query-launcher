WITH

  users_sous_secteur AS (
  SELECT
    LOWER(TRIM(sous_secetur.user)) AS user,
    sous_secetur.id_fournisseur,
    sous_secetur.id_marque,
    sous_secetur.id_secteur,
    sous_secetur.id_sous_secteur
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_user_sous_secteur` AS sous_secetur ),
    
  insights_ventes_agg AS(
  SELECT
    DISTINCT ventes.*,
    ref_sous_secteur.user,
    'externe' AS user_type
  FROM (
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg`) ventes
  JOIN
    users_sous_secteur ref_sous_secteur
  ON
    ventes.id_fournisseur = ref_sous_secteur.id_fournisseur
    AND ventes.id_marque = ref_sous_secteur.id_marque
    AND ventes.id_secteur = ref_sous_secteur.id_secteur
    AND ventes.id_sous_secteur= ref_sous_secteur.id_sous_secteur)
SELECT
  DISTINCT insights_ventes_agg.*,
  CURRENT_DATE() AS update_date
FROM
  insights_ventes_agg
LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` AS user_order_insertion
ON
  insights_ventes_agg.user = LOWER(TRIM(user_order_insertion.user))
WHERE
  user_order_insertion.insertion_date < CURRENT_DATE()