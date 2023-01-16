WITH restricted_marque_secteur AS (
    select
          lower(trim(marque.user)) as user,
         marque.id_fournisseur,
         marque.id_marque
    from
        `plateforme-fournisseurs-prod.referential_table.referential_stock_vente_fournisseur_marque` as marque
        LEFT JOIN `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` as secteur on lower(trim(marque.user)) = lower(trim(secteur.user)) 
    where
        lower(trim(secteur.user)) is NULL),
 insights_vente_agg  AS (
  SELECT
    DISTINCT ventes.*,
    ref_marque.user,
    'externe' AS user_type
  FROM
    (select * from `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg`) ventes
  JOIN
    restricted_marque_secteur ref_marque
  ON
    ventes.id_fournisseur = ref_marque.id_fournisseur
    AND ventes.id_marque = ref_marque.id_marque)

SELECT
  DISTINCT insights_vente_agg.*,
  CURRENT_DATE() as update_date
FROM
  insights_vente_agg
LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_user_order_insertion`  AS user_order_insertion
  on lower(trim(insights_vente_agg.user)) = lower(trim(user_order_insertion.user)) 
  where user_order_insertion.insertion_date < CURRENT_DATE()