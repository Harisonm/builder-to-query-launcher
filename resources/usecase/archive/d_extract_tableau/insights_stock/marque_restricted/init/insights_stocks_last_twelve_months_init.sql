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
  insights_stock_last_12_months AS(
  SELECT
    DISTINCT stocks.*,
    ref_marque.user,
    'externe' AS user_type
  FROM
    (select * from `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months`) stocks
  JOIN
    restricted_marque_secteur ref_marque
  ON
    stocks.id_fournisseur = ref_marque.id_fournisseur
    AND stocks.id_marque = ref_marque.id_marque)

SELECT
  DISTINCT insights_stock_last_12_months.*,
  CURRENT_DATE() as update_date
FROM
  insights_stock_last_12_months
LEFT JOIN
  `plateforme-fournisseurs-prod.preprod.referential_user_order_insertion`  AS user_order_insertion
  on lower(trim(insights_stock_last_12_months.user)) = lower(trim(user_order_insertion.user)) 
  where user_order_insertion.insertion_date < CURRENT_DATE()