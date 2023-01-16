WITH
  base AS (
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg`
  WHERE
    date BETWEEN '2020-01-01'
    AND CURRENT_DATE()
    AND category_1 = 'LOGICIELS & JEUX'
  UNION ALL
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg`
  WHERE
    date BETWEEN '2020-01-01'
    AND CURRENT_DATE()
    AND category_1 = 'TV, PHOTO, VIDÉO'
  UNION ALL
  SELECT
    *
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_mot_cle_agg`
  WHERE
    date BETWEEN '2020-01-01'
    AND CURRENT_DATE()
    AND category_1 = 'AUDIO, HIFI, CASQUE'),
  p2_base AS (
  SELECT
    *
  FROM
    base
  WHERE
    MOD(CAST(RAND()*10 AS INT64), 100) = 1 ),
  marques AS (
  SELECT
    DISTINCT id_marque
  FROM
    p2_base ),
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
    p2_base ),
  produits_ano AS (
  SELECT
    id_marque,
    id_produit,
    CONCAT('produit_', ROW_NUMBER() OVER (PARTITION BY id_marque ORDER BY RAND())) AS id_produit_ano
  FROM
    produits)
SELECT
  p2_base.* EXCEPT(id_marque,
    id_produit,
    ref_id_produit,
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
  id_produit_ano AS id_produit,
  CONCAT(id_marque_ano,'_',id_produit_ano) AS ref_id_produit,
FROM
  p2_base
JOIN
  produits_ano
ON
  produits_ano.id_marque = p2_base.id_marque
  AND produits_ano.id_produit = p2_base.id_produit
JOIN
  marques_ano
ON
  p2_base.id_marque = marques_ano.id_marque