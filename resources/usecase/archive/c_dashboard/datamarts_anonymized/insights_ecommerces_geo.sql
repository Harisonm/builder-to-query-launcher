WITH
  base AS (
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_geo`
  WHERE
    MOD(CAST(RAND()*1000000 AS INT64), 100) = 1),
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
    product_type,
    ref_id_produit),
  id_marque_ano AS id_marque,
  base.id_marque AS id_marque_unanonymised,
  id_produit_ano AS id_produit,
  id_produit_ano AS product_type,
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