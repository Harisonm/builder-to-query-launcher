SELECT
  CAST(import_date AS DATE) AS import_date,
  CURRENT_DATE() AS insertion_date,
  CAST(REGEXP_EXTRACT(niveau_aggregation, r'([0-9]+)') AS INT64) AS niveau_aggregation,
  id_produit,
  id_produit_fnac,
  enseigne,
  id_segment,
  id_famille,
  id_departement,
  id_secteur,
  id_sous_secteur,
  id_marque,
  CAST(UPPER(groupe_pdt) AS STRING) AS librayon,
  segment,
  reference,
  secteur,
  sous_secteur,
  famille,
  departement_vente,
  CAST(UPPER(type_pdt) AS STRING) AS type_produit,
  CONCAT(id_produit,' - ',reference) AS ref_id_produit,
  marque_full,
  REPLACE('{kpi_group_name}','_',' ') AS kpi_group,
  REPLACE('{field_name}','_',' ') AS kpi,
  ref_client.kpi_code AS kpi_ordre,
  CAST(nb_clients AS INT64) AS nb_clients_total,
  CAST(SAFE_DIVIDE(SAFE_MULTIPLY({field_name},nb_clients),100) AS INT64) AS kpi_valeur
FROM
  client LEFT JOIN `plateforme-fournisseurs-prod.referential_table.kpi_codes` as ref_client
      ON REPLACE('{kpi_group_name}','_',' ')=ref_client.kpi_group AND REPLACE('{field_name}','_',' ')=ref_client.kpi
UNION ALL