WITH 
t_last_import_date as (select max(import_date) as last_import_date_all from `plateforme-fournisseurs-prod.datamart.insights_stock`),
last_day AS (SELECT
    max(import_date) AS import_date, last_import_date_all, enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur 
    from  `plateforme-fournisseurs-prod.datamart.insights_stock`
    cross join t_last_import_date
    where 
    import_date < last_import_date_all
    group by 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

SELECT
  *
FROM
  `plateforme-fournisseurs-prod.datamart.insights_stock` base JOIN last_day using(import_date, enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur)
where  (base.rupture = false or import_date = DATE_SUB(last_import_date_all, INTERVAL 1 DAY))
and base.stock_disponible_a_date != 0