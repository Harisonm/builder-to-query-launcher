# -*- coding: utf-8 -*-
from __future__ import annotations
from src.apps.model.datamart_builder import DatamartBuilder
from src.apps.model.referential_table_builder import ReferentialTableBuilder
from src.apps.utils.bq_utils import BqUtils
from src.apps.utils.sensor.sensor_date import SensorDate
from src.apps.utils.sensor.sensor_row import SensorRow
from src.apps.utils.data_catch_up import DataCatchUp

import logging
import sys


class BqManager(object):
    def __init__(
        self,
        build_type,
        entity_name,
        action_utils,
        use_case_target,
        date_begin,
        date_end,
        field_name_sensor,
        path_destination_bq,
    ):
        """
        BqManager: Factory class that manages which class to call according to the input arguments
        Args:
            build_type (str): Type of construction to do, datamart, a_referential_table or utils (big-usecase utilities)
            entity_name (str): Name of the class to call to manage use cases
            date_begin (str): start date of the rows to be taken in BigQuery
            date_end (str): end date of the rows to be taken in BigQuery
            path_destination_bq (str): project-id.dataset-id.table-id format value
        """
        self.__param = self.build_param(
            build_type,
            entity_name,
            action_utils,
            use_case_target,
            date_begin,
            date_end,
            field_name_sensor,
            path_destination_bq,
        )

    def call_entity(self) -> None:
        """
        call_entity: factory function that allows to call classes and functions dynamically
        Returns:
                None
        """
        try:
            if self.__param.get("build_type") == "b_datamarts" or self.__param.get("build_type") == "c_dashboard" or self.__param.get("build_type") == "d_extract_tableau":
                getattr(
                    globals()["DatamartBuilder"](param=self.__param), "some_operation"
                )
            if self.__param.get("build_type") == "a_referential_table":
                getattr(
                    globals()["ReferentialTableBuilder"](param=self.__param),
                    "some_operation",
                )
            elif self.__param.get("build_type") == "utils":
                getattr(
                    globals()[self.__param.get("entity_name")](param=self.__param),
                    "some_operation_util",
                )

        except OSError as err:
            logging.error("OS error: {0}".format(err))
        except ValueError:
            logging.error(ValueError)
        except Exception as e:
            tb = sys.exc_info()
            logging.error("File or Class : {error} not Found".format(error=e.with_traceback(tb[2])))

    def class_not_found(self):
        """
        Function using to print error in logging if class search is not found
        Returns:

        """
        logging.error("No Class " + self.__param.get("entity_name") + " Found!")

    @staticmethod
    def build_param(
        build_type: str = "",
        entity_name: str = "",
        action_utils: str = "",
        use_case_target: str = "",
        date_begin: str = "",
        date_end: str = "",
        field_name_sensor: str = "",
        path_destination_bq: str = "",
    ) -> dict:
        """
        build_param : Building the parameter dictionary for child classes
        Args:
            field_name_sensor (str): Name of field in table BigQuery using for sensor date
            build_type (str): Type of construction to do, datamart, a_referential_table or utils (big-usecase utilities)
            entity_name (str): Name of the class to call to manage use cases
            action_utils (str): Variable using for utils Manager
            use_case_target (str): Name of use-case class target for utils manager (a_referential_table, b_datamarts,b_datamarts or other)
            date_begin (str): start date of the rows to be taken in BigQuery
            date_end (str): end date of the rows to be taken in BigQuery
            path_destination_bq (str): project-id.dataset-id.table-id format value
        Returns:
            dict
        """
        try:
            project_id, dataset_id, table_id = (
                path_destination_bq.split(".")
                if len(path_destination_bq.split(".")) == 3
                else (path_destination_bq + ".").split(".")
            )
            return {
                "build_type": build_type,
                "entity_name": entity_name,
                "action_utils": action_utils,
                "use_case_target": use_case_target,
                "field_name_sensor": field_name_sensor,
                "date_begin": date_begin,
                "date_end": date_end,
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_id": table_id,
            }
        except OSError as err:
            logging.error("OS error: {0}".format(err))
        except ValueError:
            logging.error("Path destination is {v}".format(v=ValueError))
