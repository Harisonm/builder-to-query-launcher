SELECT
  ventes.*,
  ref.user,
  'externe' AS user_type
FROM
  `plateforme-fournisseurs-prod.c_dashboard.insights_ventes_agg` ventes
JOIN ((
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur_backup`
    UNION ALL
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur`) EXCEPT DISTINCT (
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur` INTERSECT DISTINCT
    SELECT
      *
    FROM
      `plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur_backup`)) ref
ON
  ref.id_fournisseur = ventes.id_fournisseur