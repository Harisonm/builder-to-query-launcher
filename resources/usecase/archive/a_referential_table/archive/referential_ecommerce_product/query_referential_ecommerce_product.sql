WITH
  niv0_darty_search AS (
  SELECT
    DISTINCT day AS import_date,
    UPPER(brand) AS marque_full,
    product_id AS ecommerce_product_id,
    CASE
      WHEN REGEXP_CONTAINS(product_id, r'(mk)[0-9]+') THEN REGEXP_EXTRACT(product_id, r'(mk)[0-9]+')
      WHEN REGEXP_CONTAINS(product_id, r'(MP-)[0-9]+') THEN REGEXP_EXTRACT(product_id, r'(MP-)[0-9]+')
      WHEN REGEXP_CONTAINS(product_id, r'(MK)[0-9]+') THEN REGEXP_EXTRACT(product_id, r'(MK)[0-9]+')
    ELSE
    '1-'
  END
    AS plateform_key,
    CASE
      WHEN REGEXP_CONTAINS(product_id, r'mk([0-9]+)') THEN REGEXP_EXTRACT(product_id, r'mk([0-9]+)')
      WHEN REGEXP_CONTAINS(product_id, r'MP-([0-9]+)') THEN REGEXP_EXTRACT(product_id, r'MP-([0-9]+)')
      WHEN REGEXP_CONTAINS(product_id, r'MK([0-9]+)') THEN REGEXP_EXTRACT(product_id, r'MK([0-9]+)')
    ELSE
    product_id
  END
    AS product_id_formatted,
    SAFE_CAST(UPPER(product_name) AS STRING) AS product_name,
    'online' AS group_canal,
    'FNAC' AS enseigne,
  FROM
    `plateforme-fournisseurs-prod.data.fnac_search`
  WHERE
    day = "{date_begin}"),
  niv1_fnac_darty_ecommerce AS (
  SELECT
    *
  FROM
    niv0_darty_search
  UNION ALL
  SELECT
    DISTINCT day AS import_date,
    UPPER(brand) AS marque_full,
    product_id AS ecommerce_product_id,
    CASE
      WHEN REGEXP_CONTAINS(product_id, r'(mk)[0-9]+') THEN REGEXP_EXTRACT(product_id, r'(mk)[0-9]+')
      WHEN REGEXP_CONTAINS(product_id, r'(MP-)[0-9]+') THEN REGEXP_EXTRACT(product_id, r'(MP-)[0-9]+')
      WHEN REGEXP_CONTAINS(product_id, r'(MK)[0-9]+') THEN REGEXP_EXTRACT(product_id, r'(MK)[0-9]+')
    ELSE
    '1-'
  END
    AS plateform_key,
    CASE
      WHEN REGEXP_CONTAINS(product_id, r'mk([0-9]+)') THEN REGEXP_EXTRACT(product_id, r'mk([0-9]+)')
      WHEN REGEXP_CONTAINS(product_id, r'MP-([0-9]+)') THEN REGEXP_EXTRACT(product_id, r'MP-([0-9]+)')
      WHEN REGEXP_CONTAINS(product_id, r'MK([0-9]+)') THEN REGEXP_EXTRACT(product_id, r'MK([0-9]+)')
    ELSE
    product_id
  END
    AS product_id_formatted,
    SAFE_CAST(UPPER(product_name) AS STRING) AS product_name,
    'online' AS group_canal,
    'DARTY' AS enseigne
  FROM
    `plateforme-fournisseurs-prod.data.darty_search`
  WHERE
    day = "{date_begin}"),
  niv1_ecommerce_referential_table AS (
  SELECT
    DISTINCT niv1_fnac_darty_ecommerce.*,
    ref_marque_fournisseur.* EXCEPT(marque_full,
      enseigne,
      enseigne_stock_cible)
  FROM
    niv1_fnac_darty_ecommerce
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_fournisseur_marque` AS ref_marque_fournisseur
  USING
    (marque_full,
      enseigne)
  WHERE
    niv1_fnac_darty_ecommerce.marque_full IS NOT NULL),
  niv2_ecommerce_referential_table AS (
  SELECT
    *
  FROM
    niv1_ecommerce_referential_table
  WHERE
    REGEXP_CONTAINS(id_marque,marque_full)),
  niv3_ecommerce_referential_table AS (
  SELECT
    CAST(import_date AS DATE) AS import_date,
    CAST(ecommerce_product_id AS STRING) AS ecommerce_product_id,
    CAST(plateform_key AS STRING) AS plateform_key,
    SAFE_CAST(product_id_formatted AS INT64) AS product_id_formatted,
    CAST(product_name AS STRING) AS product_name,
    CAST(group_canal AS STRING) AS group_canal,
    CAST(enseigne AS STRING) AS enseigne,
    CAST(marque_full AS STRING) AS marque_full,
    CAST(id_marque AS STRING) AS id_marque,
    CAST(id_fournisseur AS STRING) AS id_fournisseur,
    CAST(nom_fournisseur AS STRING) AS nom_fournisseur,
    CAST(id AS INT64) AS code_fournisseur,
  FROM
    niv2_ecommerce_referential_table )
SELECT
  DISTINCT niv3_ecommerce_referential_table.*
FROM
  niv3_ecommerce_referential_table
LEFT JOIN
  `plateforme-fournisseurs-prod.{dataset_id}.referential_ecommerce_product` AS preview_ecommerce_p
USING
  ( ecommerce_product_id,
    plateform_key,
    product_id_formatted,
    product_name,
    group_canal,
    enseigne,
    marque_full)