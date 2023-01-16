# -*- coding: utf-8 -*-
from src.apps.utils.mediator.utils_mediator import UtilsMediator
import os, json
import logging
import sys


class SensorDate(UtilsMediator):
    def __init__(self, param):
        """

        Args:
            param:
        """
        super().__init__()
        self.__param = param

    def get_queries_file(self):
        try:
            with open(os.getenv("queries_config")) as f:
                data = json.load(f)
                sensor_sql_file = (
                    data.get(self.__param.get("build_type"))
                        .get(self.__param.get("entity_name"))
                        .get("sql_file")
                        .get("sql_init")
                )
            return sensor_sql_file

        except OSError as err:
            logging.error("OS error: {0}".format(err))

        except ValueError:
            logging.error(ValueError)

    @property
    def some_operation_util(self):
        try:
            sensor_sql_file = self.get_queries_file()
            sensor_query = self.open_query_file(sensor_sql_file)

            row = self.run_query(
                query=sensor_query.format(
                    project_id=self.__param.get("project_id"),
                    dataset_id=self.__param.get("dataset_id"),
                    table_id=self.__param.get("table_id"),
                    date_begin=self.__param.get("date_begin"),
                    field_name_sensor="day" if self.__param.get("field_name_sensor") is None else self.__param.get("field_name_sensor"),
                )
            )
            logging.info("{v} is found".format(v=row["last_date"][0]))

            if row["last_date"][0] == self.__param.get("date_begin"):
                logging.info(
                    "Sensor for {var} is DONE".format(
                        var=self.__param.get("project_id")
                            + "."
                            + self.__param.get("dataset_id")
                            + "."
                            + self.__param.get("table_id")
                    )
                )

            else:
                logging.info("{v} is not found".format(v=self.__param.get("date_begin")))
                sys.exit(-1)

        except OSError as err:
            logging.error("OS error: {0}".format(err))

        except ValueError:
            logging.error("Could not convert data to an integer.")

        except Exception:
            sys.exit("{date} is not last date in table {table_name}".format(date=self.__param.get("date_begin"),
                                                                            table_name=self.__param.get("table_id")))