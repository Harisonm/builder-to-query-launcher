# builder to BQ

## Using project

### Argument
positional arguments:
- build_type : Name of builder use-case, Datamart or Referential table 
- entity_name : Name of entity use-case
- path_destination_bq : project_id.dataset_id.table_id

optional arguments:
  -h, --help            show this help message and exit
- -date_begin : DATE_BEGIN, Date begin of partition date in BQ 
- -date_end DATE_END, Date end of partition date in BQ
- -table_source TABLE_SOURCE, project_id.dataset_id.table_id
- -action_utils ACTION_UTILS; Name of action for utils manager

### Values
- build_type = datamart , referential_table or utils
- entity_name = Name of entity for call query.sql from Factory Model : insights_client, InsightsVente, InsightsStock, referential_product, InsightsEcommerce,EcommerceAgg  etc ... 
  entity_name is value giving in config.json (resources/config/config.json)
- table_source =   
- date_begin = Date of query need to begin : "2020-10-09"
- date_end = Date of query need to end : "2020-10-10"
- destination_table = Path of destination query to write in BigQuery : "project_id.dataset_id.table_id"

### Configuration : 
Your can create new use-case in `resources/config/config.json` by following the existing file.

Config Json explain : 
```json
{
  "utils": {
    "SensorDate": {
      "sql_init": "resources/usecase/sensor/sensor_data/sensor_bq.sql"
    }
  },
  "a_referential_table": {
    "ReferentialEcommerceProduct": {
      "sql_init": "resources/usecase/a_referential_table/referential_ecommerce_product/query_referential_ecommerce_product_init.sql"
    },
    "sql_current": "resources/usecase/a_referential_table/referential_ecommerce_product/query_referential_ecommerce_product.sql"
  },
  ...
  "b_datamarts": {
    "InsightsEcommerce": {
      "fnac_sql": "resources/usecase/b_datamarts/insights_ecommerce/fnac/sql/query_insight_ecommerce_fnac.sql",
      "darty_sql": "resources/usecase/b_datamarts/insights_ecommerce/darty/sql/query_insight_ecommerce_darty.sql"
    }
  },
  "c_dashboard": {
    "EcommerceAgg": {
      "sql_init": "resources/usecase/c_dashboard/ecommerce/sql/insights_ecommerce_agg.sql"
    },
    ...
  }
}
```

to demystify the config.json :

```json
{
  "utils": {
    "SensorDate": {
      "give_key_name": "sql_path"
    }
  },
  "folder_name_path": {
    "entity_name": {
        "sql_file":{
            "sql_path ": "give_value"
        },
        "write_disposition": "give_value",
        "field_time_partitioning": "give_value", 
        "query_builder": "give_value"
    }
  }
}
```
- 'sql_init' key is checking from Factory Class in the case of referential tables but not for all datamarts, so the key that matches the config.json path can be any name
- 'fnac_sql' & 'darty_sql' is specific key case to insights clients.

### Launch project from python : 
Launch python from line : 
```bash
python -m src.main  <build_name> <entity_name> <source_table> -date_begin=<date_begin> -date_end=<date_end> <destination_table>
```

or 
````bash
python -m src.main  <build_name> <entity_name> <source_table> -date_begin=<date_begin> -date_end=<date_end> -action_utils=<action_utils> -table_source=<table_source> <destination_table>
````

Example : 
```bash
python -m src.main -date_begin="2019-01-01" -date_end="2019-01-30" "utils" "DataCatchUp" -action_utils="ReferentialEcommerceProduct" "plateforme-fournisseurs-prod.referential_table.referential_ecommerce_product"
```

## Datamart & Referential

Insights 
```bash
python -m src.main -date_begin="2019-01-01" "b_datamarts" "InsightsEcommerce" "project_id.dataset_id.table_id"
python -m src.main -date_begin="2021-05-01" -date_end="2021-05-31" "b_datamarts" "InsightsClient" "plateforme-fournisseurs-prod.dev.insights_clients"
```

Referential
```bash
python -m src.main -date_begin="2019-01-02" "referential_table" "ReferentialStockProduct" "plateforme-fournisseurs-prod.preprod.referential_stock_product"
```

## utils
BigQuery Tools
```bash
python -m src.main "utils" "BqUtils" -action_utils="create_dataset" "plateforme-fournisseurs-prod.c_dashboard"
```
Sensor Tools
```bash
python -m src.main -date_begin="2021-04-20" "utils" "SensorDate" "plateforme-fournisseurs-prod.data.stock"
python -m src.main -date_begin="2021-08-15" "utils" "SensorDate" -field_name_sensor="import_date" "plateforme-fournisseurs-prod.data.clients_fnac_niv4__all_versions"
python -m src.main "utils" "SensorRow" -field_name_sensor="import_date" "plateforme-fournisseurs-prod.referential_table.referential_user_fournisseur" 
```

data Catch up
```bash
python -m src.main -date_begin="2019-01-01" -date_end="2019-01-30" "utils" "DataCatchUp" -use_case_target="b_datamarts" -action_utils="InsightsEcommerce" "plateforme-fournisseurs-prod.dev.referential_ecommerce_product"
```

# Using Docker

Build Dockerfile
```bash
docker build . -t builder-to-bq
```

Run Dockerfile
```bash
docker run -p 80:80 -d builder-to-bq
```

Delete processus docker  
```bash
docker rm -f $(docker ps -a -q )
```

# License

The source code of this program is dual-licensed under the EUPL v1.2 and AGPLv3 licenses.