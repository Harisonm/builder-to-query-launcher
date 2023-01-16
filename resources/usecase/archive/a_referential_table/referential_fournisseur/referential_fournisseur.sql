SELECT
  DISTINCT ID_FRN AS id_fournisseur,
  L_RAI_SOC_GRP AS nom_fournisseur,
  D_CRE,
  D_MAJ
FROM
  `plateforme-fournisseurs-prod.referential_table.referential_fournisseur_group`
ORDER BY
  nom_fournisseur ASC