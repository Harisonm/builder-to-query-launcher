WITH
  insights_stock_last_twelve_months AS (
  SELECT 
    day AS import_date,
    UPPER(
      CASE enseigne
        WHEN 'FNAC' THEN 'FNAC'
        WHEN 'FNAC ' THEN 'FNAC'
        WHEN 'DARTY' THEN 'DARTY'
      ELSE
      enseigne
    END
      ) AS enseigne,
    CURRENT_DATE() AS insertion_date,
    id_produit,
    UPPER(CONCAT(ID_PRODUIT,' - ',reference)) AS ref_id_produit,
    CAST(CODIC AS STRING) AS codic,
    CAST(ID_PRODUIT__fnac AS STRING) AS id_produit_fnac,
    ean,
    num_article,
    CAST(N_LIG_PRD AS INT64) AS n_lig_prd,
    CAST(C_MAPPING AS STRING) AS c_mapping,
    UPPER(reference) as reference,
    UPPER(CAST(C_MARQ AS STRING)) AS c_marq,
    CAST(ID_MARQUE AS STRING) AS id_marque,
    UPPER(marque__full) as marque__full,
    id_segment,
    upper(segment) as segment,
    CAST(N_LIG_DP AS INT64) AS n_lig_dp,
    id_famille,
    upper(famille) as famille,
    id_sous_secteur,
    upper(sous_secteur) as sous_secteur,
    id_secteur,
    upper(secteur) as secteur,
    id_departement,
    departement,
    id_magasin,
    upper(magasin) as magasin,
    pays,
    CAST(LIEU_GRP AS STRING) AS lieu_group,
    upper(CASE ifnull(type_lieu, 'Entrepot')
        WHEN NULL THEN 'Entrepot'
        WHEN 'Autres' THEN 'Magasin'
        WHEN 'AUTRES' THEN 'Magasin'
      ELSE
      ifnull(type_lieu, 'Entrepot')
    END
      ) AS canal_2,
    upper(CAST(SCANAUX_STK AS STRING)) AS scanaux_stk,
    CAST(code_fournisseur AS INT64) AS code_fournisseur,
    CAST(id_fournisseur AS STRING) AS id_fournisseur,
    CAST(code_fournisseur__fnac AS INT64) AS id_fournisseur_fnac,
    CAST(fournisseur__fnac AS STRING) AS fournisseur_fnac,
    CAST(code_fournisseur__darty AS INT64) AS id_fournisseur_darty,
    CAST(fournisseur__darty AS STRING) AS fournisseur_darty,
    sum(stock_disponible) as stock_disponible_a_date    
  FROM
    `plateforme-fournisseurs-prod.data.stock`
  WHERE
    day BETWEEN DATE_SUB((SELECT MAX(day) FROM `plateforme-fournisseurs-prod.data.stock`), INTERVAL 12 MONTH)
    AND (SELECT MAX(day) FROM `plateforme-fournisseurs-prod.data.stock` )
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38

)
  SELECT
    *,
    MAX(import_date) OVER (PARTITION BY enseigne) as last_import_date,
    LAG(stock_disponible_a_date) OVER (PARTITION BY enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur ORDER BY import_date) AS quantite_stock_date_precedente,
    LAG(import_date) OVER (PARTITION BY enseigne, ID_PRODUIT, ean, num_article, id_magasin, lieu_group, canal_2, SCANAUX_STK, id_fournisseur ORDER BY import_date) AS date_precedente_stock,
    DATE_ADD(import_date, interval -1 day) as date_precedente
  FROM
    insights_stock_last_twelve_months
