WITH
  base AS (
  SELECT
    ventes_agg.* EXCEPT(enseigne),
    (
      CASE enseigne
        WHEN 'FNAC' THEN 'Magasin_group_A'
        WHEN 'DARTY' THEN 'Magasin_group_B'
      ELSE
      enseigne
    END
      ) AS enseigne,
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` AS ventes_agg
  WHERE
    date BETWEEN '2020-01-01'
    AND CURRENT_DATE()
    AND secteur = 'TVV'
  UNION ALL
  SELECT
    ventes_agg.* EXCEPT(enseigne),
    (
      CASE enseigne
        WHEN 'FNAC' THEN 'Magasin_group_A'
        WHEN 'DARTY' THEN 'Magasin_group_B'
      ELSE
      enseigne
    END
      ) AS enseigne,
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` AS ventes_agg
  WHERE
    date BETWEEN '2020-01-01'
    AND CURRENT_DATE()
    AND secteur = 'GAMING'
  UNION ALL
  SELECT
    ventes_agg.* EXCEPT(enseigne),
    (
      CASE enseigne
        WHEN 'FNAC' THEN 'Magasin_group_A'
        WHEN 'DARTY' THEN 'Magasin_group_B'
      ELSE
      enseigne
    END
      ) AS enseigne,
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` AS ventes_agg
  WHERE
    date BETWEEN '2020-01-01'
    AND CURRENT_DATE()
    AND secteur = 'PETIT ELECTROMENAGER'),
  p2_base AS (
  SELECT
    *
  FROM
    base
  WHERE
    MOD(CAST(RAND()*10 AS INT64), 100) = 1 ),
  marques AS (
  SELECT
    DISTINCT marque_full
  FROM
    p2_base ),
  marques_ano AS (
  SELECT
    marque_full,
    CONCAT('marque_', ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY RAND())) AS id_marque_ano
  FROM
    marques),
  produits AS (
  SELECT
    DISTINCT marque_full,
    id_produit
  FROM
    p2_base ),
  produits_ano AS (
  SELECT
    marque_full,
    id_produit,
    CONCAT('produit_', ROW_NUMBER() OVER (PARTITION BY marque_full ORDER BY RAND())) AS id_produit_ano
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
  s_canaux_vente AS (
  SELECT
    DISTINCT s_canaux_vente_formatted
  FROM
    p2_base ),
  s_canaux_vente_ano AS (
  SELECT
    s_canaux_vente_formatted AS id_caneaux,
    CONCAT('canaux_achats_', ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY RAND())) AS s_canaux_vente_formatted_ano
  FROM
    s_canaux_vente)
SELECT
  p2_base.* EXCEPT (id_produit,
    marque_full,
    fournisseur,
    id_fournisseur,
    reference,
    ref_id_produit,
    id_marque,
    s_canaux_vente_formatted),
  id_marque_ano AS id_marque,
  id_produit_ano AS id_produit,
  id_produit_ano AS reference,
  id_marque_ano AS marque_full,
  id_fournisseur_ano AS fournisseur,
  s_canaux_vente_ano.s_canaux_vente_formatted_ano AS s_canaux_vente_formatted,
  CONCAT(id_marque_ano,'_',id_produit_ano) AS ref_id_produit,
  id_fournisseur_ano AS id_fournisseur
FROM
  p2_base
JOIN
  produits_ano
ON
  produits_ano.marque_full = p2_base.marque_full
  AND produits_ano.id_produit = p2_base.id_produit
JOIN
  marques_ano
ON
  p2_base.marque_full = marques_ano.marque_full
JOIN
  fournisseurs_ano
ON
  p2_base.id_fournisseur = fournisseurs_ano.id_fournisseur
JOIN
  s_canaux_vente_ano
ON
  p2_base.s_canaux_vente_formatted = s_canaux_vente_ano.id_caneaux