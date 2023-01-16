WITH
  stock_p1_referential AS (
  SELECT
    CAST(stock_full.id_produit AS STRING) AS id_produit,
    CAST(ID_PRODUIT__fnac AS STRING) AS id_produit_fnac,
    CAST(stock_full.CODIC AS INT64) AS codic,
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
    (
      CASE stock_cible
        WHEN 'FNAC ' THEN 'FNAC'
        WHEN 'FNAC      ' THEN 'FNAC'
        WHEN 'DARTY' THEN 'DARTY'
      ELSE
      stock_cible
    END
      ) AS stock_cible,
    CAST(stock_full.id_segment AS INT64) AS id_segment,
    CAST(stock_full.id_famille AS INT64) AS id_famille,
    CAST(stock_full.id_departement AS INT64) AS id_departement,
    CAST(stock_full.id_secteur AS INT64) AS id_secteur,
    CAST(stock_full.id_sous_secteur AS INT64) AS id_sous_secteur,
    CAST(stock_full.ID_MARQUE AS STRING) AS id_marque,
    CAST(stock_full.segment AS STRING) AS segment,
    CAST(stock_full.reference AS STRING) AS reference,
    CAST(stock_full.secteur AS STRING) AS secteur,
    CAST(stock_full.sous_secteur AS STRING) AS sous_secteur,
    CAST(stock_full.famille AS STRING) AS famille,
    CAST(stock_full.id_fournisseur  AS STRING) AS id_fournisseur ,
    CAST(stock_full.code_fournisseur__fnac AS INT64) AS id_fournisseur_fnac,
    CAST(stock_full.fournisseur__fnac AS STRING) AS fournisseur_fnac,
    CAST(stock_full.code_fournisseur__darty AS INT64) AS id_fournisseur_darty,
    CAST(stock_full.fournisseur__darty AS STRING) AS fournisseur_darty,
    CAST(code_fournisseur AS INT64) AS code_fournisseur,
    CAST(stock_full.departement AS STRING) AS departement_vente,
    CAST(stock_full.marque__full AS STRING) AS marque_full,
    CAST(stock_full.pays AS STRING) AS pays,
    MAX(day) as date
  FROM
    `plateforme-fournisseurs-prod.data.stock` AS stock_full
    WHERE day BETWEEN "2019-01-01" AND (SELECT MAX(day) FROM `plateforme-fournisseurs-prod.data.stock`)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
  )
  , p1_uniques AS (select distinct id_produit , pays, enseigne from `plateforme-fournisseurs-prod.data.stock`)
, vente_p2_referential AS (
  SELECT  *, ROW_NUMBER() OVER (PARTITION BY enseigne, id_produit, pays, stock_p1_referential.id_fournisseur  ORDER BY DATE DESC) as row_order FROM  stock_p1_referential
)
, deduplicate as (select * except(date, row_order) from vente_p2_referential where row_order = 1)
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
ON deduplicate.id_fournisseur = ref_group.ID_FRN AND deduplicate.code_fournisseur=ref_group.N_FRN