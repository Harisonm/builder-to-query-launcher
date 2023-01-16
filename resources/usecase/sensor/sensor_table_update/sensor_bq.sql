(SELECT *  FROM `plateforme-fournisseurs-prod.{new_dataset}.{new_table}`
UNION ALL
SELECT * FROM `plateforme-fournisseurs-prod.{current_dataset}.{current_table}`)
EXCEPT DISTINCT
(SELECT * FROM `plateforme-fournisseurs-prod.{current_dataset}.{current_table}`
INTERSECT DISTINCT
SELECT * FROM `plateforme-fournisseurs-prod.{new_dataset}.{new_table}`)