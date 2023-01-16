#!/bin/bash
# date_begin and date_end is using just for log
cd /home/publisher/builder-to-bq/current
/usr/local/bin/python3.8 -m src.main -date_begin="2019-01-01" -date_end=$(date +\%Y-\%m-\%d -d "yesterday") "referential_table" "referential_product" "plateforme-fournisseurs-prod.XXXX.referential_vente_product"