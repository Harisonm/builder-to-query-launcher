WITH
  p1_ecommerce AS (
  SELECT
    day AS date,
    SAFE_CAST('FNAC' AS STRING) AS enseigne,
    SAFE_CAST(product_id AS STRING) AS id_produit,
    SAFE_CAST(category_1 AS STRING) AS category_1,
    SAFE_CAST(category_2 AS STRING) AS category_2,
    SAFE_CAST(category_3 AS STRING) AS category_3,
    SAFE_CAST(channel AS STRING) AS channel,
    SAFE_CAST(UPPER(brand) AS STRING) AS id_marque,
    SAFE_CAST(product_name AS STRING) AS product_type,
    SAFE_CAST(country AS STRING) AS pays,
    SAFE_CAST(region AS STRING) AS region,
    SAFE_CAST(city AS STRING) AS ville,
    SAFE_CAST(product_name AS STRING) AS product_name,
    SUM(SAFE_CAST(turnover AS FLOAT64)) AS ca_facture,
    SUM(SAFE_CAST(nbr_purchases AS INT64)) AS nbr_transaction,
    SUM(SAFE_CAST(units AS INT64)) AS nbr_quantite_achete,
    NULL AS nbr_ajouts_au_panier,
    NULL AS nbr_vues
  FROM
    `plateforme-fournisseurs-prod.data.fnac_achats`
  WHERE
    day ='{date_begin}'
    AND product_id IS NOT NULL
    AND product_id!=''
  GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13),
  p1_trafic AS (
  SELECT
    day AS date,
    'FNAC' AS enseigne,
    product_id AS id_produit,
    SAFE_CAST(category_1 AS STRING) AS category_1,
    SAFE_CAST(category_2 AS STRING) AS category_2,
    SAFE_CAST(category_3 AS STRING) AS category_3,
    SAFE_CAST(channel AS STRING) AS channel,
    SAFE_CAST(UPPER(brand) AS STRING) AS id_marque,
    SAFE_CAST(product_name AS STRING) AS product_type,
    country AS pays,
    region,
    city AS ville,
    SAFE_CAST(product_name AS STRING) AS product_name,
    CAST(NULL AS FLOAT64) AS ca_facture,
    NULL AS nbr_transaction,
    NULL AS nbr_quantite_achete,
    SUM(SAFE_CAST(fnac_trafic.nbr_cart_additions AS INT64)) AS nbr_ajouts_au_panier,
    SUM(SAFE_CAST(fnac_trafic.nbr_views AS INT64)) AS nbr_vues
  FROM
    `plateforme-fournisseurs-prod.data.fnac_trafic` AS fnac_trafic
  WHERE
    day ='{date_begin}'
    AND fnac_trafic.product_id IS NOT NULL
    AND fnac_trafic.product_id!=''
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
    p2_ecommerce AS (
        SELECT *
        FROM
        p1_ecommerce
        UNION ALL
         SELECT *
        FROM
        p1_trafic),
p3_ecommerce AS (
    SELECT
DISTINCT *
    EXCEPT(ca_facture,
    nbr_transaction,
    nbr_quantite_achete,
    nbr_ajouts_au_panier,
    nbr_vues),
SUM(ca_facture) OVER(PARTITION BY date,enseigne,id_produit,category_1,category_2,category_3,channel,id_marque,product_type,pays,region,ville,product_name) ca_facture,
SUM(nbr_transaction) OVER(PARTITION BY date,enseigne,id_produit,category_1,category_2,category_3,channel,id_marque,product_type,pays,region,ville,product_name) nbr_transaction,
SUM(nbr_quantite_achete) OVER(PARTITION BY date,enseigne,id_produit,category_1,category_2,category_3,channel,id_marque,product_type,pays,region,ville,product_name) nbr_quantite_achete,
SUM(nbr_ajouts_au_panier) OVER(PARTITION BY date,enseigne,id_produit,category_1,category_2,category_3,channel,id_marque,product_type,pays,region,ville,product_name) nbr_ajouts_au_panier,
SUM(nbr_vues) OVER(PARTITION BY date,enseigne,id_produit,category_1,category_2,category_3,channel,id_marque,product_type,pays,region,ville,product_name) nbr_vues
FROM p2_ecommerce),
 p4_ecommerce AS (
  SELECT
    date,
    ca_facture,
    nbr_transaction,
    nbr_quantite_achete,
    nbr_ajouts_au_panier,
    nbr_vues,
    (CASE channel
        WHEN '' THEN 'AUTRES'
        WHEN ' ' THEN 'AUTRES'
        WHEN null THEN 'AUTRES'
      ELSE
      SAFE_CAST(UPPER(channel) AS STRING)
    END
      ) AS channel,
    enseigne,
    id_produit,
    (CASE product_type
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(product_type) AS STRING)
    END
      )AS product_type,
    CONCAT(UPPER(id_produit),' - ',UPPER(product_type)) AS ref_id_produit,
    (CASE category_1
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(category_1) AS STRING)
    END
      ) AS category_1,
    (CASE category_2
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(category_2) AS STRING)
    END
      ) AS category_2,
    (CASE category_3
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(category_3) AS STRING)
    END
      ) AS category_3,
    (CASE id_marque
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(id_marque) AS STRING)
    END
      )AS id_marque,
    (CASE pays
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(pays) AS STRING)
    END
      )AS pays,
    (CASE region
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(region) AS STRING)
    END
      )AS region,
    (CASE ville
        WHEN '' THEN NULL
        WHEN ' ' THEN NULL
      ELSE
      SAFE_CAST(UPPER(ville) AS STRING)
    END
      )AS ville
  FROM
    p3_ecommerce )

SELECT
  DISTINCT
  date,
SUM(ca_facture) AS ca_facture,
  SUM(nbr_transaction) AS nbr_transaction,
  SUM(nbr_quantite_achete) AS nbr_quantite_achete,
  SUM(nbr_ajouts_au_panier) AS nbr_ajouts_au_panier,
  SUM(nbr_vues) AS nbr_vues,
  channel,
  enseigne,
  id_produit,
  product_type,
  ref_id_produit,
  category_1,
  category_2,
  category_3,
  id_marque,
  pays,
  region,
  ville
FROM
  p4_ecommerce
GROUP BY
  date,
  channel,
  enseigne,
  id_produit,
  product_type,
  ref_id_produit,
  category_1,
  category_2,
  category_3,
  id_marque,
  pays,
  region,
  ville