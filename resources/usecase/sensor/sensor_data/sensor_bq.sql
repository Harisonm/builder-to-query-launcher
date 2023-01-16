WITH
  sensor_table AS(
  SELECT
    MAX(DATE({field_name_sensor})) AS max_date
  FROM
    `{project_id}.{dataset_id}.{table_id}`)
SELECT
  CAST(max_date AS STRING) as last_date
FROM
  sensor_table
WHERE
  max_date='{date_begin}'