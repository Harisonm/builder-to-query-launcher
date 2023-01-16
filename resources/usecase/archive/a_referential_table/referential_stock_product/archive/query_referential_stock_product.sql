WITH
  p1_referential_stock_darty AS (
  SELECT
    DISTINCT day,
    CAST(stock_full.ID_PRODUIT AS STRING) AS id_produit,
    CAST(ID_PRODUIT__fnac AS STRING) AS id_produit_fnac,
    CAST(stock_full.CODIC AS INT64) AS codic,
    CAST(stock_full.ean AS INT64) AS ean,
    CAST(stock_full.enseigne AS STRING) AS enseigne,
    CAST(stock_full.stock_cible AS STRING) AS enseigne_stock_cible,
    CAST(stock_full.id_segment AS INT64) AS id_segment,
    CAST(stock_full.id_famille AS INT64) AS id_famille,
    CAST(stock_full.id_departement AS INT64) AS id_departement,
    CAST(stock_full.id_secteur AS INT64) AS id_secteur,
    CAST(stock_full.id_sous_secteur AS INT64) AS id_sous_secteur,
    CAST(stock_full.ID_MARQUE AS STRING) AS id_marque,
    CAST(stock_full.code_fournisseur__fnac AS INT64) AS id_fournisseur_fnac,
    CAST(stock_full.fournisseur__fnac AS STRING) AS fournisseur_fnac,
    CAST(stock_full.code_fournisseur__darty AS INT64) AS id_fournisseur_darty,
    CAST(stock_full.fournisseur__darty AS STRING) AS fournisseur_darty,
    CAST(id_fournisseur AS STRING) AS id_fournisseur,
    CAST(code_fournisseur AS INT64) AS code_fournisseur,
    CAST(stock_full.id_magasin AS INT64) AS id_magasin,
    CAST(stock_full.segment AS STRING) AS segment,
    CAST(stock_full.reference AS STRING) AS reference,
    CAST(stock_full.secteur AS STRING) AS secteur,
    CAST(stock_full.sous_secteur AS STRING) AS sous_secteur,
    CAST(stock_full.famille AS STRING) AS famille,
    CAST(stock_full.departement AS STRING) AS departement_vente,
    CAST(stock_full.marque__full AS STRING) AS marque_full,
    CAST(stock_full.magasin AS STRING) AS nom_magasin,
    CAST(stock_full.pays AS STRING) AS pays,
    CAST(LIEU_GRP AS STRING) AS lieu_grp,
    CAST(type_lieu AS STRING) AS type_canal,
    CAST(SCANAUX_STK AS STRING) AS canneaux_stock
  FROM
    `plateforme-fournisseurs-prod.data.stock` AS stock_full
  WHERE
    day="{date_begin}"
    AND stock_full.enseigne='DARTY'),
  p2_referential_stock_darty AS (
  SELECT
    DISTINCT p1_referential_stock_darty.*,
    CAST(lieu_darty.LLIEU AS STRING) AS lieu_magasin,
    CAST(LIEU AS STRING) AS ville,
    CAST(lieu_darty.LCOMMUNE AS STRING) AS commune,
    SAFE_CAST(lieu_darty.CPOSTAL AS INT64) AS code_postal
  FROM
    p1_referential_stock_darty
  LEFT JOIN
    `plateforme-fournisseurs-prod.tmp.full_lieux_darty` AS lieu_darty
  ON
    LIEU=nom_magasin
    AND id_magasin=CODE),
  p3_referential_stock_darty AS (
  SELECT
    DISTINCT p2_referential_stock_darty.*,
    SAFE_CAST(data_gouv_geo.code_departement AS INT64) AS code_departement,
    SAFE_CAST(UPPER(data_gouv_geo.nom_departement) AS STRING) nom_departement,
    SAFE_CAST(data_gouv_geo.code_region AS INT64) code_region,
    SAFE_CAST(UPPER(data_gouv_geo.nom_region) AS STRING) nom_region,
  FROM
    p2_referential_stock_darty
  LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.communes_departement_region` AS data_gouv_geo
  ON
    p2_referential_stock_darty.code_postal=data_gouv_geo.code_postal),
  p4_referential_stock_darty AS (
  SELECT
    DISTINCT p3_referential_stock_darty.*
  FROM
    p3_referential_stock_darty
  LEFT JOIN
    `plateforme-fournisseurs-prod.{dataset_id}.referential_stock_product` AS stock_preview
  USING
    (id_produit,
      id_produit_fnac,
      codic,
      ean,
      enseigne,
      enseigne_stock_cible,
      id_segment,
      id_famille,
      id_departement,
      id_secteur,
      id_sous_secteur,
      id_marque,
      id_fournisseur_fnac,
      fournisseur_fnac,
      id_fournisseur_darty,
      fournisseur_darty,
      id_fournisseur,
      code_fournisseur,
      id_magasin,
      segment,
      reference,
      secteur,
      sous_secteur,
      famille,
      departement_vente,
      marque_full,
      nom_magasin,
      pays)
      WHERE stock_preview.id_produit IS NULL)
SELECT
  *
FROM
  p4_referential_stock_darty
UNION ALL (
    WITH
  p1_referential_stock_fnac AS (
  SELECT
    DISTINCT day,
    CAST(stock_full.ID_PRODUIT AS STRING) AS id_produit,
    CAST(ID_PRODUIT__fnac AS STRING) AS id_produit_fnac,
    CAST(stock_full.CODIC AS INT64) AS codic,
    CAST(stock_full.ean AS INT64) AS ean,
    CAST(stock_full.enseigne AS STRING) AS enseigne,
    CAST(stock_full.stock_cible AS STRING) AS enseigne_stock_cible,
    CAST(stock_full.id_segment AS INT64) AS id_segment,
    CAST(stock_full.id_famille AS INT64) AS id_famille,
    CAST(stock_full.id_departement AS INT64) AS id_departement,
    CAST(stock_full.id_secteur AS INT64) AS id_secteur,
    CAST(stock_full.id_sous_secteur AS INT64) AS id_sous_secteur,
    CAST(stock_full.ID_MARQUE AS STRING) AS id_marque,
    CAST(stock_full.code_fournisseur__fnac AS INT64) AS id_fournisseur_fnac,
    CAST(stock_full.fournisseur__fnac AS STRING) AS fournisseur_fnac,
    CAST(stock_full.code_fournisseur__darty AS INT64) AS id_fournisseur_darty,
    CAST(stock_full.fournisseur__darty AS STRING) AS fournisseur_darty,
    CAST(id_fournisseur AS STRING) AS id_fournisseur,
    CAST(code_fournisseur AS INT64) AS code_fournisseur,
    CAST(stock_full.id_magasin AS INT64) AS id_magasin,
    CAST(stock_full.segment AS STRING) AS segment,
    CAST(stock_full.reference AS STRING) AS reference,
    CAST(stock_full.secteur AS STRING) AS secteur,
    CAST(stock_full.sous_secteur AS STRING) AS sous_secteur,
    CAST(stock_full.famille AS STRING) AS famille,
    CAST(stock_full.departement AS STRING) AS departement_vente,
    CAST(stock_full.marque__full AS STRING) AS marque_full,
    CAST(stock_full.magasin AS STRING) AS nom_magasin,
    CAST(stock_full.pays AS STRING) AS pays,
    CAST(LIEU_GRP AS STRING) AS lieu_grp,
    CAST(type_lieu AS STRING) AS type_canal,
    CAST(SCANAUX_STK AS STRING) AS canneaux_stock
  FROM
    `plateforme-fournisseurs-prod.data.stock` AS stock_full
  WHERE
    day="{date_begin}"
    AND stock_full.enseigne='FNAC'),
    p2_referential_stock_fnac AS (
  SELECT
    DISTINCT p1_referential_stock_fnac.*,
    CAST(lieu_fnac.L_UNI_GES AS STRING) AS lieu_magasin,
    CAST(UPPER(VILLE) AS STRING) AS ville,
    CAST(lieu_fnac.L_UNI_GES_CRT AS STRING) AS commune,
    SAFE_CAST(lieu_fnac.CD_POSTAL AS INT64) AS code_postal
  FROM
    p1_referential_stock_fnac
  LEFT JOIN
    `plateforme-fournisseurs-prod.tmp.full_lieux_fnac` AS lieu_fnac
  ON
    L_UNI_GES=nom_magasin
    AND id_magasin=N_UNI_GES),
  p3_referential_stock_fnac AS (
  SELECT
    DISTINCT p2_referential_stock_fnac.*,
    SAFE_CAST(data_gouv_geo.code_departement AS INT64) AS code_departement,
    SAFE_CAST(UPPER(data_gouv_geo.nom_departement) AS STRING) nom_departement,
    SAFE_CAST(data_gouv_geo.code_region AS INT64) code_region,
    SAFE_CAST(UPPER(data_gouv_geo.nom_region) AS STRING) nom_region,
  FROM
    p2_referential_stock_fnac
    LEFT JOIN
    `plateforme-fournisseurs-prod.referential_table.communes_departement_region` AS data_gouv_geo
  ON
    p2_referential_stock_fnac.code_postal=data_gouv_geo.code_postal),
  p4_referential_stock_fnac AS (
  SELECT
    DISTINCT p3_referential_stock_fnac.*
  FROM
    p3_referential_stock_fnac
  LEFT JOIN
    `plateforme-fournisseurs-prod.{dataset_id}.referential_stock_product` stock_preview
  USING
    (id_produit,
      id_produit_fnac,
      enseigne,
      enseigne_stock_cible,
      id_segment,
      id_famille,
      id_departement,
      id_secteur,
      id_sous_secteur,
      id_marque,
      id_fournisseur_fnac,
      fournisseur_fnac,
      id_fournisseur,
      code_fournisseur,
      id_magasin,
      segment,
      reference,
      secteur,
      sous_secteur,
      famille,
      departement_vente,
      marque_full,
      nom_magasin,
      pays)
      WHERE stock_preview.id_produit IS NULL)
SELECT
  DISTINCT *
FROM
  p4_referential_stock_fnac
)