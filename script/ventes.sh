#!/bin/bash
# date_begin and date_end is using just for log
cd /home/publisher/builder-to-bq/current
/usr/local/bin/python3.8 -m src.main -date_begin=$(date +\%Y-\%m-\%d -d "yesterday -5 days")  "b_datamarts" "InsightsVente" "plateforme-fournisseurs-prod.XXXX.insights_ventes"