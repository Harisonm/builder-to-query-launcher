WITH
precedent_suivant AS (
  SELECT
    *,
    LEAD(stock_disponible_a_date) OVER (PARTITION BY enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur ORDER BY import_date) AS quantite_stock_date_suivante,
    LEAD(import_date) OVER (PARTITION BY enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur ORDER BY import_date) AS date_suivante_stock,
    DATE_ADD(import_date, interval 1 day) as date_suivante
  FROM
    datamart.insights_stock_step1
  ),
ruptures_reprises as (
  select * ,
  if((quantite_stock_date_suivante is null
  # and import_date <> last_import_date
  ) or (date_suivante_stock <> date_suivante), true, false) as rupture ,
  if(quantite_stock_date_precedente is null or date_precedente_stock <> date_precedente, true, false) as reprise 
  from precedent_suivant
),
mouvements as (
  select * from ruptures_reprises where
  stock_disponible_a_date <> quantite_stock_date_precedente
  OR reprise is True 
  ),
ruptures as (
  select * except (import_date, stock_disponible_a_date),
    DATE_ADD(import_date, interval 1 day) as import_date,
    0 as stock_disponible_a_date,
    from ruptures_reprises where 
      rupture is True),
datamart as (
SELECT
  * except(quantite_stock_date_suivante, quantite_stock_date_precedente),
  LAG(stock_disponible_a_date) OVER (PARTITION BY enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur ORDER BY import_date) AS quantite_stock_date_precedente,
  LEAD(stock_disponible_a_date) OVER (PARTITION BY enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur ORDER BY import_date) AS quantite_stock_date_suivante,
  COALESCE(LAG(import_date) OVER (PARTITION BY enseigne, ID_PRODUIT, ean, num_article, id_magasin, canal_2, SCANAUX_STK, id_fournisseur ORDER BY import_date), import_date) AS date_mouvement_precedent,
  stock_disponible_a_date - quantite_stock_date_precedente AS stock_mouvement,
FROM
  (select * except (import_date, stock_disponible_a_date), import_date, stock_disponible_a_date from mouvements 
    union all (select * from ruptures)
    )
),
p1_ref_fournisseur AS (
  SELECT
    DISTINCT datamart.* EXCEPT (id_fournisseur,
      id_fournisseur_fnac,
      fournisseur_fnac,
      id_fournisseur_darty,
      fournisseur_darty),
    CAST(ref_prod.id_fournisseur AS STRING) AS id_fournisseur,
    CAST(ref_prod.nom_fournisseur AS STRING) AS fournisseur
  FROM
    datamart
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_stock_product` AS ref_prod
  ON
    datamart.id_fournisseur=ref_prod.id_fournisseur),
  p1_ref_geo AS (
  SELECT
    p1_ref_fournisseur.*,
    upper(CAST(referential_stock_geo.ville AS STRING)) AS ville,
    CAST(referential_stock_geo.code_postal AS STRING) AS code_postal,
    SAFE_CAST(referential_stock_geo.code_departement AS INT64) AS code_departement_geo,
    upper(SAFE_CAST(referential_stock_geo.nom_departement AS STRING)) nom_departement_geo,
    SAFE_CAST(referential_stock_geo.code_region AS INT64) code_region_geo,
    upper(SAFE_CAST(referential_stock_geo.nom_region AS STRING)) nom_region_geo,
  FROM
    p1_ref_fournisseur
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_stock_magasins_geo` AS referential_stock_geo
  ON
    p1_ref_fournisseur.id_magasin=referential_stock_geo.id_magasin )
  SELECT * FROM p1_ref_geo
#  order by import_date