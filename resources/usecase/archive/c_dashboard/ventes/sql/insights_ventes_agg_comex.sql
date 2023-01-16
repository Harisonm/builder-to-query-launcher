SELECT
  date,
  insertion_date,
  enseigne,
  id_famille,
  id_departement,
  id_secteur,
  id_sous_secteur,
  id_marque,
  canal_vente,
  s_canaux_vente_formatted,
  secteur,
  sous_secteur,
  famille,
  departement_vente,
  marque_full,
  SUM(CA_facture_jour) AS CA_facture_jour,
  SUM(CA_commande_jour) AS CA_commande_jour,
  SUM(nbr_transaction) AS nbr_transaction,
  SUM(quantite_facturee_jour) AS quantite_facturee_jour,
  SUM(quantite_commandee_jour) AS quantite_commandee_jour
FROM
  `plateforme-fournisseurs-prod.datamart.insights_ventes`
WHERE
    pays='FRA'
GROUP BY
  date,
  insertion_date,
  enseigne,
  id_famille,
  id_departement,
  id_secteur,
  id_sous_secteur,
  id_marque,
  canal_vente,
  s_canaux_vente_formatted,
  secteur,
  sous_secteur,
  famille,
  departement_vente,
  marque_full