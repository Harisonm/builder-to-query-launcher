SELECT
  DISTINCT
  N_FRN_FNAC AS id_fournisseur,
  FRN_FNAC AS nom_fournisseur,
  'FNAC' AS enseigne
FROM
  `plateforme-fournisseurs-prod.tmp.ventes__V_DIM_GRP_PRD_PRODUIT`
UNION ALL
SELECT
  DISTINCT
  N_FRN_DARTY AS id_fournisseur,
  FRN_DARTY AS nom_fournisseur,
  'DARTY' AS enseigne
FROM
  `plateforme-fournisseurs-prod.tmp.ventes__V_DIM_GRP_PRD_PRODUIT`