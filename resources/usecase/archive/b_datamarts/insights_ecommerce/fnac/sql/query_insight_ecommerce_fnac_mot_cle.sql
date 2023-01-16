SELECT
     day AS date,
    SAFE_CAST(product_id AS STRING) AS id_produit,
    SAFE_CAST('FNAC' AS STRING) AS enseigne,
    SAFE_CAST(UPPER(request) AS STRING) AS mot_cle,
    SAFE_CAST(UPPER(category_1) AS STRING) AS category_1,
    SAFE_CAST(UPPER(category_2) AS STRING) AS category_2,
    SAFE_CAST(UPPER(category_3) AS STRING) AS category_3,
    SAFE_CAST(UPPER(brand) AS STRING) AS id_marque,
    SAFE_CAST(UPPER(product_name) AS STRING) AS ref_id_produit,
    SUM(SAFE_CAST(turnover AS FLOAT64)) AS ca_facture,
    SUM(SAFE_CAST(nbr_orders  AS INT64)) AS nbr_transaction,
    SUM(SAFE_CAST(nbr_views  AS INT64)) AS nbr_vues,
    SUM(SAFE_CAST(nbr_requests AS INT64)) AS nbr_requete,
    SUM(SAFE_CAST(nbr_cart_additions AS INT64)) AS nbr_ajouts_au_panier
  FROM
    `plateforme-fournisseurs-prod.data.fnac_search` AS fnac_search
  WHERE
    fnac_search.day='{date_begin}'
    AND product_id IS NOT NULL
    AND product_id!=''
    AND UPPER(request) != 'NON SEARCH'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    HAVING nbr_requete > 0 AND nbr_vues > 0