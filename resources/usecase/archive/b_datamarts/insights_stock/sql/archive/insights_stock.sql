WITH
  previous_window AS (
  SELECT
    *,
    LAG(stock_disponible) OVER (PARTITION BY enseigne, stock_cible, ID_PRODUIT, CODIC, ID_PRODUIT__fnac, ean, num_article, N_LIG_PRD, C_MAPPING, reference, C_MARQ, ID_MARQUE, marque__full, id_segment, segment, N_LIG_DP, id_famille, famille, id_sous_secteur, sous_secteur, id_secteur, secteur, id_departement, departement, id_magasin, magasin, pays, ville, code_postal, departement__geo, LIEU_GRP, type_lieu, canal_2, SCANAUX_STK, id_fournisseur, code_fournisseur, code_fournisseur__fnac, fournisseur__fnac, fournisseur__fnac, code_fournisseur__darty, fournisseur__darty ORDER BY day) AS previous_windows_stock
  FROM
    `plateforme-fournisseurs-prod.data.stock`
  WHERE
  day BETWEEN (SELECT min(day) FROM `plateforme-fournisseurs-prod.data.stock`) AND (SELECT max(day) FROM `plateforme-fournisseurs-prod.data.stock`)),
  P1_previous AS(
  SELECT
    DISTINCT day AS import_date,
    enseigne AS enseigne,
    stock_disponible AS stock_disponible_a_date,
    previous_windows_stock AS quantite_stock_date_precedent,
    stock_disponible - previous_windows_stock AS stock_mouvement,
    ID_PRODUIT,
    CODIC,
    ID_PRODUIT__fnac,
    ean,
    num_article,
    N_LIG_PRD,
    C_MAPPING,
    reference,
    C_MARQ,
    ID_MARQUE,
    marque__full,
    id_segment,
    segment,
    N_LIG_DP,
    id_famille,
    famille,
    id_sous_secteur,
    sous_secteur,
    id_secteur,
    secteur,
    id_departement,
    departement,
    id_magasin,
    magasin,
    pays,
    ville,
    code_postal,
    departement__geo,
    LIEU_GRP,
    type_lieu,
    canal_2,
    SCANAUX_STK,
    code_fournisseur,
    id_fournisseur,
    code_fournisseur__fnac,
    fournisseur__fnac,
    code_fournisseur__darty,
    fournisseur__darty
  FROM
    previous_window
  WHERE
    stock_disponible <> previous_windows_stock
    OR previous_windows_stock IS NULL),
  p2_stock AS (
  SELECT
    import_date,
    CURRENT_DATE() AS insertion_date,
    P1_previous.enseigne,
    CAST(P1_previous.ID_PRODUIT AS STRING) AS id_produit,
    CONCAT(P1_previous.ID_PRODUIT,' - ',P1_previous.reference) AS ref_id_produit,
    stock_disponible_a_date,
    quantite_stock_date_precedent,
    stock_mouvement,
    CAST(P1_previous.CODIC AS STRING) AS codic,
    CAST(P1_previous.ID_PRODUIT__fnac AS STRING) AS id_produit_fnac,
    P1_previous.ean,
    P1_previous.num_article,
    CAST(P1_previous.N_LIG_PRD AS INT64) AS n_lig_prd,
    CAST(P1_previous.C_MAPPING AS STRING) AS c_mapping,
    P1_previous.reference,
    CAST(P1_previous.C_MARQ AS STRING) AS c_marq,
    CAST(P1_previous.ID_MARQUE AS STRING) AS id_marque,
    P1_previous.marque__full,
    P1_previous.id_segment,
    P1_previous.segment,
    CAST(P1_previous.N_LIG_DP AS INT64) AS n_lig_dp,
    P1_previous.id_famille,
    P1_previous.famille,
    P1_previous.id_sous_secteur,
    P1_previous.sous_secteur,
    P1_previous.id_secteur,
    P1_previous.secteur,
    P1_previous.id_departement,
    P1_previous.departement,
    P1_previous.id_magasin,
    P1_previous.magasin,
    P1_previous.pays,
    CAST(LIEU_GRP AS STRING) AS lieu_group,
    CASE type_lieu
    WHEN NULL THEN 'Entrepot'
    WHEN 'Autres' THEN 'Magasin'
    WHEN 'AUTRES' THEN 'Magasin'
    else type_lieu
  END
    canal_2,
    CAST(SCANAUX_STK AS STRING) AS scanaux_stk,
    CAST(code_fournisseur AS INT64) AS code_fournisseur,
    CAST(id_fournisseur AS STRING) AS id_fournisseur,
    CAST(code_fournisseur__fnac AS INT64) AS id_fournisseur_fnac,
    CAST(fournisseur__fnac AS STRING) AS fournisseur_fnac,
    CAST(code_fournisseur__darty AS INT64) AS id_fournisseur_darty,
    CAST(fournisseur__darty AS STRING) AS fournisseur_darty
  FROM
    P1_previous)
  SELECT
    DISTINCT
    p2_stock.*,
    CAST(referential_stock_product.ville AS STRING) AS ville,
    CAST(referential_stock_product.code_postal AS STRING) AS code_postal,
    SAFE_CAST(referential_stock_product.code_departement AS INT64) AS code_departement_geo,
    SAFE_CAST(referential_stock_product.nom_departement AS STRING) nom_departement_geo,
    SAFE_CAST(referential_stock_product.code_region AS INT64) code_region_geo,
    SAFE_CAST(referential_stock_product.nom_region AS STRING) nom_region_geo,
  FROM
    p2_stock
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_stock_product` AS referential_stock_product
  ON
    p2_stock.id_produit=referential_stock_product.ID_PRODUIT
    AND p2_stock.code_fournisseur=referential_stock_product.code_fournisseur
    AND p2_stock.id_magasin=referential_stock_product.id_magasin