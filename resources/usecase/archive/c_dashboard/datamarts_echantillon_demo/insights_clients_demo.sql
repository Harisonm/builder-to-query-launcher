WITH
  base AS (
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients`
  WHERE
    secteur = 'GAMING'
  UNION ALL
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients`
  WHERE
    secteur = 'TVV'
  UNION ALL
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_profiling_clients`
  WHERE
    secteur = 'PETIT ELECTROMENAGER' ),
  p2_base AS (
  SELECT
    *
  FROM
    base
  WHERE
    MOD(CAST(RAND()*1000000 AS INT64), 100) = 1 ),
  marques AS (
  SELECT
    DISTINCT marque_full
  FROM
    p2_base ),
  marques_ano AS (
  SELECT
    DISTINCT marque_full,
    CONCAT('marque_', ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY RAND())) AS id_marque_ano
  FROM
    marques),
  produits AS (
  SELECT
    DISTINCT marque_full,
    id_produit
  FROM
    p2_base ),
  produits_ano AS (
  SELECT
    DISTINCT marque_full,
    id_produit,
    CONCAT('produit_', ROW_NUMBER() OVER (PARTITION BY marque_full ORDER BY RAND())) AS id_produit_ano
  FROM
    produits)
SELECT
  p2_base.* EXCEPT(id_marque,
    id_produit,
    marque_full,
    reference,
    ref_id_produit,
    id_produit_fnac,
    enseigne),
  (
    CASE enseigne
      WHEN 'FNAC' THEN 'MagasinA'
      WHEN 'DARTY' THEN 'MagasinB'
    ELSE
    enseigne
  END
    ) AS enseigne,
  id_marque_ano AS id_marque,
  id_marque_ano AS marque_full,
  id_produit_ano AS id_produit,
  id_produit_ano AS reference,
  id_produit_ano AS ref_id_produit
FROM
  p2_base
JOIN
  produits_ano
ON
  produits_ano.marque_full = p2_base.marque_full
  AND produits_ano.id_produit = p2_base.id_produit
JOIN
  marques_ano
ON
  p2_base.marque_full = marques_ano.marque_full