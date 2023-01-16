WITH
  insights_ventes_agg AS (
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
    AND ventes.id_sous_secteur = ref_sous_secteur.id_sous_secteur
  ),
new_users AS (
  SELECT * FROM `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion` users 
  CROSS JOIN  (SELECT MAX(update_date) last_update_date FROM `plateforme-fournisseurs-prod.d_extract_tableau.insights_ventes_agg_production` ) last_update
  WHERE users.insertion_date > last_update_date and users.insertion_date <= CURRENT_DATE()
  )

SELECT
  DISTINCT insights_ventes_agg.*,
  CURRENT_DATE() as update_date
FROM
  insights_ventes_agg JOIN new_users ON LOWER(TRIM(new_users.user))=LOWER(TRIM(insights_ventes_agg.user))