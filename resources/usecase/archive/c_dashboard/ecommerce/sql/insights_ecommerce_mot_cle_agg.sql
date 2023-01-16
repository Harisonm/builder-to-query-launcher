WITH
  achats AS (
  SELECT
    'DARTY' AS enseigne,
    UPPER(brand) AS brand,
    product_id,
    day,
    revenue AS ca
  FROM
    `plateforme-fournisseurs-prod.data.darty_achats`
  UNION ALL (
    SELECT
      'FNAC' AS enseigne,
      UPPER(brand) AS brand,
      product_id,
      day,
      turnover AS ca
    FROM
      `plateforme-fournisseurs-prod.data.fnac_achats`)),
  every_day AS (SELECT DISTINCT day FROM achats),
  every_product AS (SELECT distinct enseigne, product_id FROM achats),
  every_product_every_day AS (SELECT * FROM every_day CROSS JOIN every_product),
  sales_everyday AS (SELECT every_product_every_day.*, COALESCE(ca, 0) AS ca FROM every_product_every_day LEFT JOIN achats USING( day, product_id, enseigne) ),
  sales_30d AS (
  SELECT
    product_id,
    day,
    SUM(ca) OVER (PARTITION BY enseigne, product_id ORDER BY day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS ca_30d
  FROM
    sales_everyday join every_day USING (day)),
  focus_products AS (
  SELECT
    DISTINCT day AS DATE,
    product_id AS id_produit
  FROM
    sales_30d
  WHERE
    ca_30d > 10)
SELECT
  date,
  SUM(ca_facture) AS ca_facture,
  SUM(nbr_transaction) AS nbr_transaction,
  SUM(nbr_ajouts_au_panier) AS nbr_ajouts_au_panier,
  SUM(nbr_vues) AS nbr_vues,
  SUM(nbr_requete) AS nbr_requete,
  enseigne,
  id_produit,
  ref_id_produit,
  mot_cle,
  category_1,
  category_2,
  category_3,
  id_marque
FROM
  `plateforme-fournisseurs-prod.datamart.insights_ecommerces_mot_cle`
  JOIN
  focus_products
USING
  (id_produit,
    date)
WHERE
  REGEXP_CONTAINS(id_produit, r'mk([0-9]+)') IS NOT TRUE
  AND REGEXP_CONTAINS(id_produit, r'MP-([0-9]+)') IS NOT TRUE
  AND REGEXP_CONTAINS(id_produit, r'MK([0-9]+)') IS NOT TRUE
  AND date ='{date_begin}'
GROUP BY
  date,
  enseigne,
  id_produit,
  ref_id_produit,
  mot_cle,
  category_1,
  category_2,
  category_3,
  id_marque
HAVING
  (nbr_vues > 0)
  OR (nbr_transaction > 0)
  OR (ca_facture > 0)
  OR (nbr_ajouts_au_panier> 0)