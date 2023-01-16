WITH
  p1_insights_vente AS (
  SELECT
    DISTINCT day,
    pays,
    CAST(SUM(ventes_full.ca_facture) AS FLOAT64) AS ca_facture_jour,
    CAST(ROUND(SUM(ventes_full.ca_commande),4) AS FLOAT64) AS ca_commande_jour,
    CAST(COUNT(ventes_full.ca_facture) AS INT64) AS nbr_transaction,
    CAST(SUM(ventes_full.quantite_facturee) AS INT64) AS quantite_facturee_jour,
    CAST(SUM(ventes_full.quantite_commandee) AS INT64) AS quantite_commandee_jour,
    CAST(ventes_full.id_produit AS STRING) AS id_produit,
    (
      CASE enseigne
        WHEN 'FNAC' THEN 'FNAC'
        WHEN 'FNAC ' THEN 'FNAC'
        WHEN 'DARTY' THEN 'DARTY'
      ELSE
      enseigne
    END
      ) AS enseigne,
    CAST(ventes_full.id_segment AS INT64) AS id_segment,
    CAST(ventes_full.id_famille AS INT64) AS id_famille,
    CAST(ventes_full.id_departement AS INT64) AS id_departement,
    CAST(ventes_full.id_secteur AS INT64) AS id_secteur,
    CAST(ventes_full.id_sous_secteur AS INT64) AS id_sous_secteur,
    CAST(ventes_full.id_marque AS STRING) AS id_marque,
    CAST(ventes_full.id_frn AS STRING) AS id_fournisseur,
    CAST(ventes_full.n_frn AS INT64) AS code_fournisseur,
    CAST(ventes_full.id_fournisseur__fnac AS INT64) AS id_fournisseur_fnac,
    (
      CASE canal_2
        WHEN ' ' THEN 'CANAL NON IDENTIFIE'
        WHEN NULL THEN 'CANAL NON IDENTIFIE'
      ELSE
      canal_2
    END
      ) AS canal_vente,
    CAST(s_canaux_vte__formatted AS STRING) AS s_canaux_vente_formatted,
    CAST(id_magasin AS INT64) AS id_magasin,
    CAST(ville AS STRING) AS ville
  FROM
    `plateforme-fournisseurs-prod.data.ventes` AS ventes_full
  GROUP BY
    pays,
    day,
    id_produit,
    enseigne,
    id_segment,
    id_famille,
    id_departement,
    id_secteur,
    id_sous_secteur,
    id_marque,
    id_fournisseur,
    n_frn,
    id_fournisseur_fnac,
    canal_vente,
    s_canaux_vte__formatted,
    id_magasin,
    ville),
  p2_insights_ventes AS (
  SELECT
    DISTINCT p1_insights_vente.day AS date,
    CURRENT_DATE() AS insertion_date,
    p1_insights_vente.id_produit,
    p1_insights_vente.ca_facture_jour,
    p1_insights_vente.ca_commande_jour,
    p1_insights_vente.nbr_transaction,
    p1_insights_vente.quantite_facturee_jour,
    p1_insights_vente.quantite_commandee_jour,
    p1_insights_vente.enseigne,
    p1_insights_vente.id_segment,
    p1_insights_vente.id_famille,
    p1_insights_vente.id_departement,
    p1_insights_vente.id_secteur,
    p1_insights_vente.id_sous_secteur,
    p1_insights_vente.id_marque,
    p1_insights_vente.code_fournisseur,
    p1_insights_vente.id_fournisseur_fnac,
    p1_insights_vente.canal_vente,
    p1_insights_vente.id_magasin,
    p1_insights_vente.s_canaux_vente_formatted,
    p1_insights_vente.ville,
    CAST(referential_vente_product.segment AS string) AS segment,
    CAST(UPPER(referential_vente_product.reference) AS string) AS reference,
    CONCAT(p1_insights_vente.id_produit,' - ',referential_vente_product.reference) AS ref_id_produit,
    CAST(referential_vente_product.secteur AS string) AS secteur,
    CAST(referential_vente_product.sous_secteur AS string) AS sous_secteur,
    CAST(referential_vente_product.famille AS string) AS famille,
    CAST(referential_vente_product.departement_vente AS string) AS departement_vente,
    CAST(referential_vente_product.marque_full AS string) AS marque_full,
    referential_vente_product.id_fournisseur,
    CAST(referential_vente_product.nom_fournisseur AS string) AS fournisseur,
    CAST(p1_insights_vente.pays AS string) AS pays,
    CAST(referential_vente_magasins.nom_magasin AS string) AS nom_magasin,
    CAST(referential_vente_magasins.code_postal AS string) AS code_postal,
    SAFE_CAST(referential_vente_magasins.code_departement AS int64) AS code_departement_geo,
    SAFE_CAST(referential_vente_magasins.nom_departement AS string) nom_departement_geo,
    SAFE_CAST(referential_vente_magasins.code_region AS int64) code_region_geo,
    SAFE_CAST(referential_vente_magasins.nom_region AS string) nom_region_geo,
  FROM
    p1_insights_vente
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_vente_product` AS referential_vente_product
  ON
    p1_insights_vente.id_produit=referential_vente_product.id_produit
    AND COALESCE(p1_insights_vente.pays,
      '') = COALESCE(referential_vente_product.pays,
      '') #IN_CASE_pays_IS_NULL
    AND p1_insights_vente.enseigne=referential_vente_product.enseigne
    AND p1_insights_vente.id_fournisseur=referential_vente_product.id_fournisseur
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_magasins_geo` AS referential_vente_magasins
  ON
    p1_insights_vente.id_magasin=referential_vente_magasins.id_magasin
    AND p1_insights_vente.enseigne=referential_vente_magasins.enseigne)
SELECT
  *
FROM
  p2_insights_ventes