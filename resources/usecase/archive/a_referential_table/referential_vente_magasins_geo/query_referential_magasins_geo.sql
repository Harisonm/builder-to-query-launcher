WITH
  ids_fnac AS (
  SELECT
    DISTINCT enseigne,
    id_magasin
  FROM
    `plateforme-fournisseurs-prod.data.ventes`
  WHERE
    enseigne = 'FNAC'
    OR enseigne = 'FNAC '),
  fnac AS (
  SELECT
    DISTINCT N_UNI_GES,
    L_UNI_GES,
    SAFE_CAST(CD_POSTAL AS INT64) CD_POSTAL
  FROM
    `plateforme-fournisseurs-prod.data.full_lieux_fnac`),
  fnac_cp AS (
  SELECT
    enseigne,
    id_magasin,
    fnac.L_UNI_GES AS nom_magasin,
    CD_POSTAL AS code_postal
  FROM
    ids_fnac
  LEFT JOIN
    fnac
  ON
    ids_fnac.id_magasin = fnac.N_UNI_GES ),
  ids_darty AS (
  SELECT
    DISTINCT enseigne,
    id_magasin
  FROM
    `plateforme-fournisseurs-prod.data.ventes`
  WHERE
    enseigne = 'DARTY'),
  darty AS (
  SELECT
    DISTINCT CODE,
    LLIEU,
    SAFE_CAST(CPOSTAL AS INT64) CPOSTAL
  FROM
    `plateforme-fournisseurs-prod.data.full_lieux_darty`),
  darty_cp AS (
  SELECT
    enseigne,
    id_magasin,
    darty.LLIEU AS nom_magasin,
    darty.CPOSTAL AS code_postal
  FROM
    ids_darty
  LEFT JOIN
    darty
  ON
    ids_darty.id_magasin = darty.CODE),
  full_ids AS (
  SELECT
    *
  FROM
    fnac_cp
  UNION ALL (
    SELECT
      *
    FROM
      darty_cp)),
  data_gouv_geo AS (
  SELECT
    DISTINCT code_postal,
    SAFE_CAST(code_departement AS INT64) AS code_departement,
    SAFE_CAST(UPPER(nom_departement) AS STRING) nom_departement,
    SAFE_CAST(code_region AS INT64) code_region,
    SAFE_CAST(UPPER(nom_region) AS STRING) nom_region,
  FROM
    `plateforme-fournisseurs-prod.referential_table.communes_departement_region`),
  data_gouv_geo_count AS (
  SELECT
    code_postal,
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    ROW_NUMBER() OVER(PARTITION BY code_postal ) AS row_rank
  FROM
    data_gouv_geo),
  data_gouv_geo_dedup AS (
  SELECT
    * EXCEPT( row_rank)
  FROM
    data_gouv_geo_count
  WHERE
    row_rank = 1),
  ref AS (
  SELECT
    full_ids.*,
    data_gouv_geo_dedup.* EXCEPT(code_postal)
  FROM
    full_ids
  LEFT JOIN
    data_gouv_geo_dedup
  ON
    full_ids.code_postal = data_gouv_geo_dedup.code_postal ),
  manual AS (
  SELECT
    enseigne,
    id_magasin,
    UPPER(nom_magasin),
    code_postal,
    code_departement,
    nom_departement,
    code_region,
    nom_region
  FROM
    `plateforme-fournisseurs-prod.referential_table.ref_magasin_manuel` ),
  ref_union AS (
  SELECT
    ref.*
  FROM
    ref
  LEFT JOIN
    manual
  ON
    ref.id_magasin = manual.id_magasin
  WHERE
    manual.id_magasin IS NULL
  UNION ALL (
    SELECT
      *
    FROM
      manual ))
SELECT
  (
    CASE enseigne
      WHEN 'FNAC' THEN 'FNAC'
      WHEN 'FNAC ' THEN 'FNAC'
      WHEN 'DARTY' THEN 'DARTY'
    ELSE
    enseigne
  END
    ) AS enseigne,
  SAFE_CAST(id_magasin AS INT64) AS id_magasin,
  SAFE_CAST(nom_magasin AS STRING) AS nom_magasin,
  SAFE_CAST(code_postal AS INT64) AS code_postal,
  SAFE_CAST(code_departement AS INT64) AS code_departement,
  SAFE_CAST(nom_departement AS STRING) AS nom_departement,
  SAFE_CAST(code_region AS INT64) AS code_region,
  SAFE_CAST(REGEXP_REPLACE(nom_region, "[']+", "")  AS STRING) AS nom_region
FROM
  ref_union