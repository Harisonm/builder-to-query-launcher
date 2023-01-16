SELECT
  * EXCEPT(date,id_produit,
    product_type,
    ca_facture,
    nbr_transaction,
    nbr_quantite_achete,
    nbr_ajouts_au_panier,
    nbr_vues,
    nbr_requete),
FROM
  `plateforme-fournisseurs-prod.c_dashboard.insights_ecommerces_agg`