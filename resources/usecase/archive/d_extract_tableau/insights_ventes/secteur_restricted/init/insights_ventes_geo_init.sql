WITH
  insights_ventes_geo AS (
  SELECT
    DISTINCT ventes.*,
    LOWER(TRIM(ref_secteur.user)) as user,
    'externe' AS user_type
  FROM
    (select * from `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_geo`) ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` ref_secteur
  ON
    ventes.id_fournisseur = ref_secteur.id_fournisseur
    AND ventes.id_marque = ref_secteur.id_marque
    AND ventes.id_secteur = ref_secteur.id_secteur)
SELECT
  DISTINCT insights_ventes_geo.*,
  CURRENT_DATE() as update_date
FROM
  insights_ventes_geo
LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion`  AS user_order_insertion
ON
  LOWER(TRIM(user_order_insertion.user))=LOWER(TRIM(insights_ventes_geo.user))
  where user_order_insertion.insertion_date < CURRENT_DATE()