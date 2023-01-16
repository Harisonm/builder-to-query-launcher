WITH
  base AS (
  SELECT
    stock_last_twelve_months.* EXCEPT(enseigne,
      canal_2),
    (
      CASE enseigne
        WHEN 'FNAC' THEN 'Magasin_group_A'
        WHEN 'DARTY' THEN 'Magasin_group_B'
      ELSE
      enseigne
    END
      ) AS enseigne,
    UPPER(canal_2) AS canal_2,
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months` AS stock_last_twelve_months
  WHERE
    import_date BETWEEN '2021-01-01'
    AND CURRENT_DATE()
    AND secteur = 'TVV'
    AND REGEXP_CONTAINS(magasin, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'DARTY([0-9]+)') IS NOT TRUE
  UNION ALL
  SELECT
    stock_last_twelve_months.* EXCEPT(enseigne,
      canal_2),
    (
      CASE enseigne
        WHEN 'FNAC' THEN 'Magasin_group_A'
        WHEN 'DARTY' THEN 'Magasin_group_B'
      ELSE
      enseigne
    END
      ) AS enseigne,
    UPPER(canal_2) AS canal_2,
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months` AS stock_last_twelve_months
  WHERE
    import_date BETWEEN '2021-01-01'
    AND CURRENT_DATE()
    AND secteur = 'GAMING'
    OR secteur = 'PETIT ELECTROMENAGER'
    AND REGEXP_CONTAINS(magasin, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'DARTY([0-9]+)') IS NOT TRUE
  UNION ALL
  SELECT
    stock_last_twelve_months.* EXCEPT(enseigne,
      canal_2),
    (
      CASE enseigne
        WHEN 'FNAC' THEN 'Magasin_group_A'
        WHEN 'DARTY' THEN 'Magasin_group_B'
      ELSE
      enseigne
    END
      ) AS enseigne,
    UPPER(canal_2) AS canal_2,
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_stock_last_twelve_months` AS stock_last_twelve_months
  WHERE
    import_date BETWEEN '2021-01-01'
    AND CURRENT_DATE()
    AND secteur = 'PETIT ELECTROMENAGER'
    AND REGEXP_CONTAINS(magasin, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(magasin, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'FNAC([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'DARTY([0-9]+)') IS NOT TRUE
    AND REGEXP_CONTAINS(ville, r'DARTY([0-9]+)') IS NOT TRUE),
  p2_base AS (
  SELECT
    *
  FROM
    base
  WHERE
    MOD(CAST(RAND()*10000 AS INT64), 100) = 1 ),
  marques AS (
  SELECT
    DISTINCT id_marque
  FROM
    p2_base ),
  marques_ano AS (
  SELECT
    id_marque,
    CONCAT('marque_', ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY RAND())) AS id_marque_ano
  FROM
    marques),
  produits AS (
  SELECT
    DISTINCT id_marque,
    id_produit
  FROM
    p2_base ),
  produits_ano AS (
  SELECT
    id_marque,
    id_produit,
    CONCAT('produit_', ROW_NUMBER() OVER (PARTITION BY id_marque ORDER BY RAND())) AS id_produit_ano
  FROM
    produits),
  fournisseurs AS (
  SELECT
    DISTINCT id_fournisseur,
    fournisseur
  FROM
    p2_base ),
  fournisseurs_ano AS (
  SELECT
    id_fournisseur,
    fournisseur,
    CONCAT('fournisseur_', ROW_NUMBER() OVER (PARTITION BY id_fournisseur ORDER BY RAND())) AS id_fournisseur_ano
  FROM
    fournisseurs),
  scanaux AS (
  SELECT
    DISTINCT scanaux_stk
  FROM
    p2_base ),
  scanaux_stk_ano AS (
  SELECT
    scanaux_stk AS id_caneaux,
    CONCAT('canaux_achats_', ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY RAND())) AS scanaux_stk_ano
  FROM
    scanaux)
SELECT
  p2_base.* EXCEPT(id_marque,
    id_produit,
    ref_id_produit,
    enseigne,
    codic,
    id_produit_fnac,
    ean,
    c_marq,
    marque__full,
    reference,
    id_fournisseur,
    fournisseur,
    scanaux_stk,
    c_mapping),
  (
    CASE enseigne
      WHEN 'FNAC' THEN 'MagasinA'
      WHEN 'DARTY' THEN 'MagasinB'
    ELSE
    enseigne
  END
    ) AS enseigne,
  id_marque_ano AS id_marque,
  id_produit_ano AS id_produit,
  id_produit_ano AS reference,
  id_marque_ano AS marque__full,
  scanaux_stk_ano.scanaux_stk_ano AS scanaux_stk,
  CONCAT(id_marque_ano,'_',id_produit_ano) AS ref_id_produit,
  id_fournisseur_ano AS id_fournisseur
FROM
  p2_base
JOIN
  produits_ano
ON
  produits_ano.id_marque = p2_base.id_marque
  AND produits_ano.id_produit = p2_base.id_produit
JOIN
  marques_ano
ON
  p2_base.id_marque = marques_ano.id_marque
JOIN
  fournisseurs_ano
ON
  p2_base.id_fournisseur = fournisseurs_ano.id_fournisseur
JOIN
  scanaux_stk_ano
ON
  p2_base.scanaux_stk = scanaux_stk_ano.id_caneaux