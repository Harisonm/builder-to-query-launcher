SELECT
  CAST(import_date AS DATE) AS import_date,
  CURRENT_DATE() AS insertion_date,
  CAST(REGEXP_EXTRACT(niveau_aggregation, r'([0-9]+)') AS INT64) AS niveau_aggregation,
  CAST(NULL AS STRING) AS id_produit,
  CAST(NULL AS STRING) AS id_produit_fnac,
  'FNAC' AS enseigne,
  NULL AS id_segment,
  NULL AS id_famille,
  NULL AS id_departement,
  NULL AS id_secteur,
  NULL AS id_sous_secteur,
  CAST(NULL AS STRING) id_marque,
  CAST(UPPER(groupe_pdt) AS STRING) AS librayon,
  CAST(NULL AS STRING) AS segment,
  CAST(NULL AS STRING) AS reference,
  CAST(NULL AS STRING) AS secteur,
  CAST(NULL AS STRING) AS sous_secteur,
  CAST(NULL AS STRING) AS famille,
  CAST(NULL AS STRING) AS departement_vente,
  CAST(UPPER(type_pdt) AS STRING) AS type_produit,
  CAST(NULL AS STRING) AS ref_id_produit,
  CAST(UPPER(name_brand) AS STRING) AS marque_full,
  REPLACE('{kpi_group_name}','_',' ') AS kpi_group,
  REPLACE('{field_name}','_',' ') AS kpi,
  ref_client.kpi_code AS kpi_ordre,
  CAST(nb_clients AS INT64) AS nb_clients_total,
  CAST(SAFE_DIVIDE(SAFE_MULTIPLY({field_name},nb_clients),100) AS INT64) AS kpi_valeur
FROM
  `plateforme-fournisseurs-prod.data.clients_fnac_{niv}__all_versions` AS client LEFT JOIN `plateforme-fournisseurs-prod.referential_table.kpi_codes` as ref_client
      ON REPLACE('{kpi_group_name}','_',' ')=ref_client.kpi_group AND REPLACE('{field_name}','_',' ')=ref_client.kpi
WHERE DATE(client.import_date) BETWEEN '{date_begin}' AND '{date_end}'
UNION ALL