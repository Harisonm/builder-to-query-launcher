SELECT
  date,
  enseigne,
  id_produit,
  product_type,
  ref_id_produit,
  category_1,
  category_2,
  category_3,
  id_marque,
  channel,
  SUM(ca_facture) AS ca_facture,
  SUM(nbr_transaction) AS nbr_transaction,
  SUM(nbr_quantite_achete) AS nbr_quantite_achete,
  SUM(nbr_ajouts_au_panier) AS nbr_ajouts_au_panier,
  SUM(nbr_vues) AS nbr_vues
FROM
  `plateforme-fournisseurs-prod.datamart.insights_ecommerces`
WHERE
  REGEXP_CONTAINS(id_produit, r'mk([0-9]+)') IS NOT TRUE
  AND REGEXP_CONTAINS(id_produit, r'MP-([0-9]+)') IS NOT TRUE
  AND REGEXP_CONTAINS(id_produit, r'MK([0-9]+)') IS NOT TRUE
  AND pays='FRANCE'
  AND date ='{date_begin}'
  and category_1 != 'LIVRES'
GROUP BY
  date,
  enseigne,
  id_produit,
  product_type,
  ref_id_produit,
  category_1,
  category_2,
  category_3,
  id_marque,
  channel
HAVING
  (nbr_vues > 0)
  OR (nbr_transaction > 0)
  OR (nbr_quantite_achete > 0)
  OR (ca_facture > 0)
  OR (nbr_ajouts_au_panier> 0)