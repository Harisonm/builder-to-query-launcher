#!/bin/bash
cd /home/publisher/builder-to-bq/current
/usr/local/bin/python3.8 -m src.main -date_begin=$(date +\%Y-\%m-\%d -d "yesterday") "datamart" "InsightsEcommerceMotCle" "plateforme-fournisseurs-prod.XXXX.insights_ecommerces_mot_cle"