WITH
  top_produits as (SELECT id_produit, count(1) as mv_count  FROM `plateforme-fournisseurs-prod.datamart.insights_stock` where stock_mouvement != 0 group by id_produit order by mv_count desc limit 5000)
  ,base_last AS (
  SELECT
    stock.*
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_stock`stock
  join top_produits on stock.id_produit = top_produits.id_produit
  where stock.import_date = '2021-04-20')
  ,base_prev AS (
  SELECT
    stock.*
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_stock`stock
  join top_produits on stock.id_produit = top_produits.id_produit
  where MOD(CAST(RAND()*1000000 AS INT64), 200) = 1 and import_date < '2021-04-20'),
  base as (select * from base_last UNION ALL SELECT * from base_prev),
  marques AS (
  SELECT
    DISTINCT id_marque
  FROM
    base ),
  marques_ano AS (
  SELECT
    id_marque,
    CONCAT('marque_', ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY RAND())) AS id_marque_ano
  FROM
    marques),
  produits AS (
  SELECT
    DISTINCT id_marque,
    id_produit
  FROM
    base ),
  produits_ano AS (
  SELECT
    id_marque,
    id_produit,
    CONCAT('produit_', ROW_NUMBER() OVER (PARTITION BY id_marque ORDER BY RAND())) AS id_produit_ano
  FROM
    produits)
SELECT
  base.* EXCEPT(id_marque,
    id_produit,
    marque__full,
    reference,
    ref_id_produit,
    code_fournisseur),
  id_marque_ano AS id_marque,
  id_marque_ano AS marque__full,
  id_produit_ano AS id_produit,
  id_produit_ano AS reference,
  id_produit_ano AS ref_id_produit
FROM
  base
JOIN
  produits_ano
ON
  produits_ano.id_marque = base.id_marque
  AND produits_ano.id_produit = base.id_produit
JOIN
  marques_ano
ON
  base.id_marque = marques_ano.id_marque