SELECT
  *
FROM (
  SELECT
    CURRENT_DATE() AS insertion_date,
    * EXCEPT (insertion_date),
    '' AS user,
    'interne' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg`)
UNION ALL (
  SELECT
    CURRENT_DATE() AS insertion_date,
    ventes.* EXCEPT (insertion_date),
    ref.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` ref
  ON
    ref.id_fournisseur = ventes.id_fournisseur)
UNION ALL (
  SELECT
    CURRENT_DATE() AS insertion_date,
    ventes.* EXCEPT (insertion_date),
    ref_fam.user,
    'externe' AS user_type
  FROM
    `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
  JOIN
    `plateforme-fournisseurs-prod.referential_table.referential_user_famille` ref_fam
  ON
    ref_fam.famille = ventes.famille)