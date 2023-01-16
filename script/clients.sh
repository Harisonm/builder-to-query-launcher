#!/bin/bash
cd /home/publisher/builder-to-bq/current
/usr/local/bin/python3.8 -m src.main -date_begin=$(date +\%Y-\%m-\%d -d "-0 month -$(($(date +\%d)-1)) days") -date_end=$(date +\%Y-\%m-\%d -d "-$(date +%d) days +1 month") "datamart" "InsightsClient" "plateforme-fournisseurs-prod.XXXX.insights_profiling_clients"