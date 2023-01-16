WITH
  niv0_darty_search AS (
  SELECT
    DISTINCT
    day AS import_date,
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
    day = "2019-01-01"),
  niv1_fnac_darty_ecommerce AS (
  SELECT
    *
  FROM
    niv0_darty_search
  UNION ALL
  SELECT
    DISTINCT
    day AS import_date,
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
    day = "2019-01-01")
SELECT * FROM niv1_fnac_darty_ecommerce