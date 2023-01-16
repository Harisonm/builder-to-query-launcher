WITH user_order_insertion AS (SELECT LOWER(TRIM(user)) AS user,fournisseur_ordre_priority.insertion_date AS insertion_date
FROM (
  SELECT  fournisseur, DATE(insertion_date) AS insertion_date
  FROM `plateforme-fournisseurs-prod.referential_table.referential_fournisseur_ordre_priority`) fournisseur_ordre_priority
JOIN `plateforme-fournisseurs-prod.referential_table.referential_user_secteur` referential_user_secteur
ON lower(referential_user_secteur.user) LIKE CONCAT('%', lower(fournisseur_ordre_priority.fournisseur), '%')
GROUP BY user,fournisseur_ordre_priority.insertion_date
UNION ALL
SELECT LOWER(TRIM(user)) AS user,fournisseur_ordre_priority.insertion_date AS insertion_date
FROM (
  SELECT  fournisseur, DATE(insertion_date) AS insertion_date
  FROM `plateforme-fournisseurs-prod.referential_table.referential_fournisseur_ordre_priority`) fournisseur_ordre_priority
JOIN `plateforme-fournisseurs-prod.referential_table.referential_stock_vente_fournisseur_marque` referential_user_marque
ON lower(referential_user_marque.user) LIKE CONCAT('%', lower(fournisseur_ordre_priority.fournisseur), '%')
GROUP BY user,fournisseur_ordre_priority.insertion_date
ORDER BY 2 ASC)
SELECT DISTINCT * FROM user_order_insertion ORDER BY insertion_date ASC