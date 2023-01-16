WITH
  vente_p1_referential AS (
  SELECT
    CAST(ventes_full.id_produit AS STRING) AS id_produit,
    CAST(ID_PRODUIT__fnac AS STRING) AS id_produit_fnac,
    CAST(ventes_full.CODIC AS INT64) AS codic,
    CAST(ean AS INT64) AS ean,
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
    CAST(ventes_full.ID_MARQUE AS STRING) AS id_marque,
    CAST(ventes_full.segment AS STRING) AS segment,
    CAST(ventes_full.reference AS STRING) AS reference,
    CAST(ventes_full.secteur AS STRING) AS secteur,
    CAST(ventes_full.sous_secteur AS STRING) AS sous_secteur,
    CAST(ventes_full.famille AS STRING) AS famille,
    CAST(ventes_full.ID_FRN AS STRING) AS id_fournisseur,
    CAST(ventes_full.N_FRN AS INT64) AS numero_fournisseur,
    CAST(ventes_full.departement AS STRING) AS departement_vente,
    CAST(ventes_full.marque__full AS STRING) AS marque_full,
    CAST(ventes_full.pays AS STRING) AS pays,
    MAX(day) AS date,
  FROM
    `plateforme-fournisseurs-prod.data.ventes` AS ventes_full
  WHERE
    day BETWEEN "2019-01-01"
    AND (
    SELECT
      MAX(day)
    FROM
      `plateforme-fournisseurs-prod.data.ventes`)
  GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21),
  p1_uniques AS (
  SELECT
    DISTINCT id_produit,
    pays,
    enseigne
  FROM
    `plateforme-fournisseurs-prod.data.ventes`),
  vente_p2_referential AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY enseigne, id_produit, pays, id_fournisseur ORDER BY date DESC) AS row_order
  FROM
    vente_p1_referential ),
  deduplicate AS (
  SELECT
    * EXCEPT(date,
      row_order)
  FROM
    vente_p2_referential
  WHERE
    row_order = 1)
SELECT DISTINCT
  deduplicate.id_produit,
  deduplicate.id_produit_fnac ,
  deduplicate.codic,
  deduplicate.ean,
  deduplicate.enseigne,
  deduplicate.id_segment,
  deduplicate.id_famille,
  deduplicate.id_departement,
  deduplicate.id_secteur,
  deduplicate.id_sous_secteur,
  deduplicate.id_marque,
  deduplicate.segment,
  deduplicate.reference,
  deduplicate.secteur,
  deduplicate.sous_secteur,
  deduplicate.famille,
  CAST(ref_group.ID_FRN AS STRING) AS id_fournisseur,
  CAST(UPPER(ref_group.L_RAI_SOC_GRP) AS STRING) AS nom_fournisseur,
  deduplicate.departement_vente,
  deduplicate.marque_full,
  deduplicate.pays
FROM
  deduplicate
 LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_fournisseur_group` AS ref_group
ON deduplicate.id_fournisseur = ref_group.ID_FRN