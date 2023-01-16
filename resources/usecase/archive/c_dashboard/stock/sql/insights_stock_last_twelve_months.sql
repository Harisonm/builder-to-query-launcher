WITH
  base AS (
  SELECT
    *,
  IF
    ((quantite_stock_date_precedente = 0)
      AND (date_mouvement_precedent IS NOT NULL),
      DATE_DIFF(import_date, date_mouvement_precedent, DAY),
      0) AS nb_jours_rupture
  FROM
    `plateforme-fournisseurs-prod.datamart.insights_stock` 
 ),
extract_produit as (
  SELECT distinct enseigne, id_produit, reference, ref_id_produit, famille, id_famille
  FROM    `plateforme-fournisseurs-prod.datamart.insights_stock`  ),
extract_produit_ordered as (
  SELECT *, row_number() over (partition by enseigne, id_produit) as row_count from extract_produit),
ref_produit as (
  SELECT * from extract_produit_ordered where row_count = 1),
full_cal AS (
  SELECT
    DISTINCT import_date
  FROM
    base),
  template_no_cal AS (
  SELECT
    DISTINCT * EXCEPT (
      import_date,
      ean, 
      famille, 
      id_famille,
      codic,
      id_produit_fnac,
      n_lig_prd,
      c_mapping,
      n_lig_dp,
      num_article, 
      lieu_group, 
      SCANAUX_STK,
      date_mouvement_precedent,
      insertion_date,
      nb_jours_rupture,
      stock_disponible_a_date,
      stock_mouvement,
      quantite_stock_date_precedente,
      quantite_stock_date_suivante,
      date_suivante ,
      date_precedente,
      date_suivante_stock,
      date_precedente_stock ,
      rupture,
      reprise,
      id_segment,
      segment,
      reference,
      ref_id_produit,
      magasin)
  FROM
    base),
  template_everyday AS (
  SELECT
    *
  FROM
    template_no_cal
  JOIN
    full_cal
  ON
    1=1),
  everyday_incomplete AS (
  SELECT
    template_everyday.*,
    date_mouvement_precedent,
    stock_disponible_a_date,
    quantite_stock_date_precedente
  FROM
    template_everyday
  LEFT JOIN
    base
  USING
    (id_produit,
      id_magasin,
      canal_2,
      import_date,
      enseigne) ),
  everyday_complete AS (
  SELECT
    a.* EXCEPT (stock_disponible_a_date,
      date_mouvement_precedent,
      quantite_stock_date_precedente),
      coalesce(a.stock_disponible_a_date,
      b.quantite_stock_date_precedente) AS stock_disponible_a_date,
    coalesce(a.date_mouvement_precedent,
      b.date_mouvement_precedent) AS date_mouvement_precedent
  FROM
    everyday_incomplete a
  LEFT JOIN
    base b
  ON
    a.id_produit = b.id_produit
    AND a.enseigne = b.enseigne
    AND a.canal_2 = b.canal_2
    AND a.id_fournisseur = b.id_fournisseur
    AND a.id_magasin = b.id_magasin
    AND a.import_date < b.import_date
    AND a.import_date > b.date_mouvement_precedent),
agg_magasin as (SELECT
  import_date,
  enseigne,
  id_produit,
  ref_id_produit,
  reference,
  c_marq,
  id_marque,
  marque__full,
  id_famille,
  famille,
  id_sous_secteur,
  sous_secteur,
  id_secteur,
  secteur,
  pays,
  #lieu_group,
  canal_2,
  #scanaux_stk,
  code_fournisseur,
  id_fournisseur,
  fournisseur,
  SUM(
  IF
    (stock_disponible_a_date=0,
      1,
      0)) AS nb_jours_rupture,
  SUM(stock_disponible_a_date ) AS stock_disponible_a_date
FROM
  everyday_complete
  join ref_produit using (enseigne, id_produit)
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11, 12,13,14,15,16,17,18,19
  having stock_disponible_a_date is not null
),
mapping_canaux as (
  select * except (canal_2),
  if(substr(canal_2, 0, 7)='MAGASIN', 'MAGASIN', canal_2) as canal_2
  from agg_magasin
)
select * except(stock_disponible_a_date, nb_jours_rupture),
sum(stock_disponible_a_date) as stock_disponible_a_date ,
sum(nb_jours_rupture) as nb_jours_rupture
from mapping_canaux
where canal_2 in ('MAGASIN', 'ENTREPOT')
group by 1,2,3,4,5,6,7,8,9,10,11, 12,13,14,15,16,17,18,19
