WITH
  ref_vente_darty AS (
  SELECT
    DISTINCT day AS import_date,
    SAFE_CAST((CASE
          WHEN REGEXP_CONTAINS(id_produit, r'@([0-9]+)') THEN REGEXP_EXTRACT(id_produit, r'@([0-9]+)')
        ELSE
        id_produit
      END
        ) AS INT64) AS product_id_formatted,
    '1-' AS plateform_key,
    CONCAT(codic,'_',reference) AS reference,
    enseigne,
    'offline' AS group_canal,
    CAST(code_fournisseur AS INT64) AS code_fournisseur,
    fournisseur AS nom_fournisseur,
    id_fournisseur,
    marque_full,
    id_marque,
  FROM
    `plateforme-fournisseurs-prod.referential_table.referential_vente_product`
  WHERE
    enseigne='DARTY'
    AND REGEXP_CONTAINS(id_marque,marque_full)
    AND day='2019-01-01')
SELECT
  *
FROM
  ref_vente_darty
UNION ALL (
  WITH
    ref_vente_fnac AS (
    SELECT
      DISTINCT day AS import_date,
      CAST(id_produit_fnac AS INT64) AS product_id_formatted,
      '1-' AS plateform_key,
      CONCAT(id_produit_fnac,'_',reference) AS reference,
      enseigne,
      'offsite' AS group_canal,
      CAST(code_fournisseur AS INT64) AS code_fournisseur,
      fournisseur AS nom_fournisseur,
      id_fournisseur,
      marque_full,
      id_marque,
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_vente_product`
    WHERE
      enseigne='FNAC'
      AND REGEXP_CONTAINS(id_marque,marque_full)
      AND day='2019-01-01')
  SELECT
    *
  FROM
    ref_vente_fnac )
UNION ALL (
  WITH
    niv0_ref_ecommerce AS (
    SELECT
      DISTINCT import_date,
      SAFE_CAST(product_id_formatted AS INT64) AS product_id_formatted,
      plateform_key,
      CAST(product_name AS STRING) AS reference,
      enseigne,
      group_canal,
      SAFE_CAST(code_fournisseur AS INT64) AS code_fournisseur,
      nom_fournisseur,
      id_fournisseur,
      marque_full,
      id_marque,
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_ecommerce_product`
    WHERE
      product_id_formatted IS NOT NULL
      AND import_date='2019-01-01')
  SELECT
    *
  FROM
    niv0_ref_ecommerce )