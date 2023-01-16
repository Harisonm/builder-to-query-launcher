SELECT
  date,
  insertion_date,
  id_produit,
  enseigne,
  id_segment,
  id_famille,
  id_departement,
  id_secteur,
  id_sous_secteur,
  id_marque,
  id_fournisseur,
  id_fournisseur_fnac,
  canal_vente,
  s_canaux_vente_formatted,
  segment,
  reference,
  ref_id_produit,
  secteur,
  sous_secteur,
  famille,
  departement_vente,
  marque_full,
  fournisseur,
  pays,
  ville,
  code_postal,
  code_departement_geo,
  nom_departement_geo,
  code_region_geo,
  nom_region_geo,
  SUM(CA_facture_jour) AS CA_facture_jour,
  SUM(CA_commande_jour) AS CA_commande_jour,
  SUM(nbr_transaction) AS nbr_transaction,
  SUM(quantite_facturee_jour) AS quantite_facturee_jour,
  SUM(quantite_commandee_jour) AS quantite_commandee_jour
FROM
  `plateforme-fournisseurs-prod.datamart.insights_ventes`
WHERE
  date BETWEEN DATE_SUB((
    SELECT
      MAX(date)
    FROM
      `plateforme-fournisseurs-prod.datamart.insights_ventes`), INTERVAL 12 MONTH)
  AND (
  SELECT
    MAX(date)
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_ventes`)
GROUP BY
  date,
  insertion_date,
  id_produit,
  enseigne,
  id_segment,
  id_famille,
  id_departement,
  id_secteur,
  id_sous_secteur,
  id_marque,
  id_fournisseur,
  id_fournisseur_fnac,
  canal_vente,
  s_canaux_vente_formatted,
  segment,
  reference,
  ref_id_produit,
  secteur,
  sous_secteur,
  famille,
  departement_vente,
  marque_full,
  fournisseur,
  code_postal,
  pays,
  ville,
  code_departement_geo,
  nom_departement_geo,
  code_region_geo,
  nom_region_geo