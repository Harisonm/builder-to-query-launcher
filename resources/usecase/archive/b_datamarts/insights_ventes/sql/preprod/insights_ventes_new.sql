WITH
  p0_ventes AS (
  SELECT
    ventes_full.* EXCEPT(ID_FRN,
      N_FRN,
      id_fournisseur,
      fournisseur,
      id_fournisseur__fnac),
    ref_frn.N_FRN_FNAC AS id_fournisseur,
    ref_frn.FRN_FNAC AS fournisseur
  FROM
    `plateforme-fournisseurs-prod.data.ventes` AS ventes_full
  LEFT JOIN
    `plateforme-fournisseurs-prod.tmp.ventes__V_DIM_GRP_PRD_PRODUIT` AS ref_frn
  ON
    ventes_full.ID_PRODUIT=ref_frn.ID_PRODUIT
    AND ventes_full.ID_MARQUE=ref_frn.ID_MARQUE
  WHERE
    ventes_full.enseigne='FNAC'
  UNION ALL
  SELECT
    ventes_full.* EXCEPT(ID_FRN,
      N_FRN,
      id_fournisseur,
      fournisseur,
      id_fournisseur__fnac),
    ref_frn.N_FRN_DARTY AS id_fournisseur,
    ref_frn.FRN_DARTY AS fournisseur
  FROM
    `plateforme-fournisseurs-prod.data.ventes` AS ventes_full
  LEFT JOIN
    `plateforme-fournisseurs-prod.tmp.ventes__V_DIM_GRP_PRD_PRODUIT` AS ref_frn
  ON
    ventes_full.ID_PRODUIT=ref_frn.ID_PRODUIT
    AND ventes_full.ID_MARQUE=ref_frn.ID_MARQUE
  WHERE
    ventes_full.enseigne='DARTY'),
  p1_ventes AS (
  SELECT
    DISTINCT day,
    CAST(SUM(ventes_full.CA_facture) AS FLOAT64) AS CA_facture_jour,
    CAST(ROUND(SUM(ventes_full.CA_commande),4) AS FLOAT64) AS CA_commande_jour,
    CAST(COUNT(ventes_full.CA_facture) AS INT64) AS nbr_transaction,
    CAST(SUM(ventes_full.quantite_facturee) AS INT64) AS quantite_facturee_jour,
    CAST(SUM(ventes_full.quantite_commandee) AS INT64) AS quantite_commandee_jour,
    CAST(ventes_full.ID_PRODUIT AS STRING) AS id_produit,
    CAST(ventes_full.enseigne AS STRING) AS enseigne,
    CAST(ventes_full.id_segment AS INT64) AS id_segment,
    CAST(ventes_full.id_famille AS INT64) AS id_famille,
    CAST(ventes_full.id_departement AS INT64) AS id_departement,
    CAST(ventes_full.id_secteur AS INT64) AS id_secteur,
    CAST(ventes_full.id_sous_secteur AS INT64) AS id_sous_secteur,
    CAST(ventes_full.ID_MARQUE AS STRING) AS id_marque,
    CAST(ventes_full.id_fournisseur AS INT64) AS id_fournisseur,
    CAST(ventes_full.fournisseur AS STRING) AS fournisseur,
    (CASE canal_2
        WHEN ' ' THEN 'CANAL NON IDENTIFIE'
        WHEN NULL THEN 'CANAL NON IDENTIFIE'
      ELSE
      canal_2
    END
      ) AS canal_vente,
    CAST(S_CANAUX_VTE__formatted AS STRING) AS s_canaux_vente_formatted,
    CAST(id_magasin AS INT64) AS id_magasin
  FROM
    p0_ventes AS ventes_full
  WHERE
    day BETWEEN '2019-01-01'
    AND '2021-06-28'
  GROUP BY
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
    fournisseur,
    canal_vente,
    S_CANAUX_VTE__formatted,
    id_magasin),
  p2_ventes AS (
  SELECT
    DISTINCT p1_insights_vente.day AS date,
    CURRENT_DATE() AS insertion_date,
    p1_insights_vente.id_produit,
    p1_insights_vente.CA_facture_jour,
    p1_insights_vente.CA_commande_jour,
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
    p1_insights_vente.id_fournisseur,
    p1_insights_vente.fournisseur,
    p1_insights_vente.canal_vente,
    p1_insights_vente.s_canaux_vente_formatted,
    CAST(referential_vente_product.segment AS STRING) AS segment,
    CAST(UPPER(referential_vente_product.reference) AS STRING) AS reference,
    CONCAT(p1_insights_vente.id_produit,' - ',referential_vente_product.reference) AS ref_id_produit,
    CAST(referential_vente_product.secteur AS STRING) AS secteur,
    CAST(referential_vente_product.sous_secteur AS STRING) AS sous_secteur,
    CAST(referential_vente_product.famille AS STRING) AS famille,
    CAST(referential_vente_product.departement_vente AS STRING) AS departement_vente,
    CAST(referential_vente_product.marque_full AS STRING) AS marque_full,
    CAST(referential_vente_product.nom_magasin AS STRING) AS nom_magasin,
    CAST(referential_vente_product.pays AS STRING) AS pays,
    CAST(referential_vente_product.ville AS STRING) AS ville,
    CAST(referential_vente_product.code_postal AS STRING) AS code_postal,
    SAFE_CAST(referential_vente_product.code_departement AS INT64) AS code_departement_geo,
    SAFE_CAST(referential_vente_product.nom_departement AS STRING) nom_departement_geo,
    SAFE_CAST(referential_vente_product.code_region AS INT64) code_region_geo,
    SAFE_CAST(referential_vente_product.nom_region AS STRING) nom_region_geo,
  FROM
    P1_ventes AS p1_insights_vente
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_vente_product` AS referential_vente_product
  ON
    p1_insights_vente.id_produit=referential_vente_product.ID_PRODUIT
    AND p1_insights_vente.id_magasin=referential_vente_product.id_magasin
    AND p1_insights_vente.enseigne=referential_vente_product.enseigne)
SELECT
  *
FROM
  P2_ventes