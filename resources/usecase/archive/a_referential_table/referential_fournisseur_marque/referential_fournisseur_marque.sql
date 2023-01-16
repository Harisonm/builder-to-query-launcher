WITH
  stock_p1_referential AS (
  SELECT
    TRIM(enseigne) AS enseigne,
    CAST(stock_full.ID_MARQUE AS STRING) AS id_marque,
    CAST(stock_full.id_fournisseur AS STRING) AS id_fournisseur,
    CAST(stock_full.code_fournisseur AS INT64) AS numero_fournisseur,
    CAST(stock_full.marque__full AS STRING) AS marque_full,
    MAX(day) AS date,
  FROM
    `plateforme-fournisseurs-prod.data.stock` AS stock_full
  WHERE
    day BETWEEN "2019-01-01"
    AND (
    SELECT
      MAX(day)
    FROM
      `plateforme-fournisseurs-prod.data.stock`)
  GROUP BY
    1,2,3,4,5),
  stock_p2_referential AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY enseigne, id_fournisseur, id_marque ORDER BY date DESC) AS row_order
  FROM
    stock_p1_referential ),
  deduplicate AS (
  SELECT
    * EXCEPT(date,
      row_order)
  FROM
    stock_p2_referential
  WHERE
    row_order = 1),
final_data AS (
SELECT DISTINCT
  deduplicate.numero_fournisseur,
  CAST(UPPER(ref_group.L_RAI_SOC_GRP) AS STRING) AS nom_fournisseur,
  CAST(ref_group.ID_FRN AS STRING) AS id_fournisseur,
  deduplicate.marque_full,
  deduplicate.id_marque,
  deduplicate.enseigne,

FROM
  deduplicate

 LEFT JOIN
  `plateforme-fournisseurs-prod.referential_table.referential_fournisseur_group` AS ref_group
ON deduplicate.id_fournisseur = ref_group.ID_FRN

order by id_fournisseur)

select * from final_data where id_fournisseur is not null and marque_full is not null