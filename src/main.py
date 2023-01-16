from src.apps.manager.entity_factory import BqManager
from dotenv import load_dotenv, find_dotenv
import argparse
import logging
import json, os

logging.basicConfig(level=logging.INFO)

load_dotenv(find_dotenv())

if __name__ == "__main__":
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument(
        "-date_begin", type=str, help="Date begin of partition date in BQ"
    )

    my_parser.add_argument(
        "-date_end", type=str, help="Date end of partition date in BQ"
    )

    my_parser.add_argument(
        "-table_source", type=str, help="project_id.dataset_id.table_id"
    )

    my_parser.add_argument(
        "build_type",
        type=str,
        help="Name of builder use-case, Datamart or Referential table",
    )

    my_parser.add_argument("entity_name", type=str, help="Name of entity use-case")

    my_parser.add_argument(
        "-use_case_target",
        type=str,
        help="Name of use-case class target for utils manager (a_referential_table, b_datamarts,b_datamarts or other)",
    )

    my_parser.add_argument(
        "-action_utils",
        type=str,
        help="Name of use-case Table target for utils manager (ReferentialEcommerceProduct, ReferentialVenteProduct..)",
    )

    my_parser.add_argument(
        "-field_name_sensor",
        type=str,
        help="Name of field in table BigQuery using for sensor date",
    )

    my_parser.add_argument(
        "path_destination_bq", type=str, help="project_id.dataset_id.table_id"
    )

    args = my_parser.parse_args()
    date_begin = args.date_begin
    date_end = args.date_end
    action_utils = args.action_utils
    use_case_target = args.use_case_target
    build_type = args.build_type
    entity_name = args.entity_name
    field_name_sensor = args.field_name_sensor
    path_destination_bq = args.path_destination_bq
    logging.info("args passed {args}".format(args=args))

    BqManager(
        build_type,
        entity_name,
        action_utils,
        use_case_target,
        date_begin,
        date_end,
        field_name_sensor,
        path_destination_bq,
    ).call_entity()
