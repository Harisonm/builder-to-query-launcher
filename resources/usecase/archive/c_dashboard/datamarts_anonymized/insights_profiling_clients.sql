WITH
  base AS (
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients`
  WHERE
    MOD(CAST(RAND()*1000000 AS INT64), 100) = 1),
  marques AS (
  SELECT
    DISTINCT marque_full
  FROM
    base ),
  marques_ano AS (
  SELECT
    DISTINCT
    marque_full,
    CONCAT('marque_', ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY RAND())) AS id_marque_ano
  FROM
    marques),
  produits AS (
  SELECT
    DISTINCT marque_full,
    id_produit
  FROM
    base ),
  produits_ano AS (
  SELECT
    DISTINCT
    marque_full,
    id_produit,
    CONCAT('produit_', ROW_NUMBER() OVER (PARTITION BY marque_full ORDER BY RAND())) AS id_produit_ano
  FROM
    produits)
SELECT
  base.* EXCEPT(id_marque,
    id_produit,
    marque_full,
    reference,
    ref_id_produit),
  id_marque_ano AS id_marque,
  base.marque_full AS id_marque_unanonymised,
  id_marque_ano AS marque_full,
  id_produit_ano AS id_produit,
  id_produit_ano AS reference,
  id_produit_ano AS ref_id_produit
FROM
  base
JOIN
  produits_ano
ON
  produits_ano.marque_full = base.marque_full
  AND produits_ano.id_produit = base.id_produit
JOIN
  marques_ano
ON
  base.marque_full = marques_ano.marque_full