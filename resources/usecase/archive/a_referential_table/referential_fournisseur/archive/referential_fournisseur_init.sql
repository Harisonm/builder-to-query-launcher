WITH
  p1_referential_fournisseur AS (
  SELECT
    DISTINCT ifnull(NULLIF(id_fournisseur_fnac, 3635),
      ifnull(id_fournisseur_darty,
        code_fournisseur)) AS id,
    ifnull(NULLIF(fournisseur_fnac, 'ETS DARTY ET FILS SAS'),
      ifnull(fournisseur_darty,
        fournisseur_fnac)) AS nom_fournisseur,
    id_fournisseur AS id_fournisseur,
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_stock_product`
  WHERE
    id_fournisseur IS NOT NULL
    AND day='2019-01-01')
SELECT DISTINCT
  *
FROM
  p1_referential_fournisseur
WHERE
  nom_fournisseur IS NOT NULL
  AND id_fournisseur!='|'
  AND id_fournisseur!='I'
  AND id!=0
  AND REGEXP_CONTAINS(id_fournisseur, CONCAT(CAST(id AS STRING),'@'))