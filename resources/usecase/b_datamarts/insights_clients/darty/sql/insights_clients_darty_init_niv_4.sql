WITH client AS (
    SELECT
  DISTINCT
  product.id_produit AS id_produit,
  product.id_produit_fnac AS id_produit_fnac,
  product.enseigne AS enseigne,
  product.id_segment AS id_segment,
  product.id_famille AS id_famille,
  product.id_departement AS id_departement,
  product.id_secteur AS id_secteur,
  product.id_sous_secteur AS id_sous_secteur,
  product.id_marque AS id_marque,
  product.segment AS segment,
  product.reference AS reference,
  product.secteur AS secteur,
  product.sous_secteur AS sous_secteur,
  product.famille AS famille,
  product.departement_vente AS departement_vente,
  product.marque_full AS marque_full,
  client.*
FROM
  `plateforme-fournisseurs-prod.data.clients_{enseigne_low}_niv4__all_versions` AS client
INNER JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_vente_product` AS product
ON
  client.codic=product.codic
  AND client.ean13=product.ean
  AND product.enseigne='{enseigne_up}'
WHERE DATE(client.import_date) BETWEEN '{date_begin}' AND '{date_end}')