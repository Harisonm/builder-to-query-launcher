# -*- coding: utf-8 -*-
from src.apps.utils.mediator.utils_mediator import UtilsMediator
import os, json
import logging
import sys


class SensorRow(UtilsMediator):
    def __init__(self, param):
        """

        Args:
            param:
        """
        super().__init__()
        self.__param = param
        self.get_param_config()

    def get_param_config(self):
        try:
            with open(os.getenv("queries_config")) as f:
                data = json.load(f)
                self.__param['sql_file'] = data.get(self.__param.get("build_type")).get(
                    self.__param.get("entity_name")).get("sql_file")

                self.__param['current_dataset'] = str(data.get(self.__param.get("build_type"))
                                                        .get(self.__param.get("entity_name"))
                                                        .get("current_dataset"))

                self.__param['current_table'] = str(data.get(self.__param.get("build_type"))
                                                        .get(self.__param.get("entity_name"))
                                                        .get("current_table"))

                self.__param['new_dataset'] = str(data.get(self.__param.get("build_type"))
                                                        .get(self.__param.get("entity_name"))
                                                        .get("new_dataset"))

                self.__param['new_table'] = str(data.get(self.__param.get("build_type"))
                                                        .get(self.__param.get("entity_name"))
                                                        .get("new_table"))

                logging.info("param using : {param}".format(param=self.__param))

        except Exception as error:
            logging.error(error)

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
                    current_dataset=self.__param.get("current_dataset"),
                    current_table=self.__param.get("current_table"),
                    new_dataset=self.__param.get("new_dataset"),
                    new_table=self.__param.get("new_table"),
                )
            )

            if row.empty:
                logging.info('Not Data diff found between two table referential')
                sys.exit()
            else:
                logging.info("{v} \n is found".format(v=row))

        except OSError as err:
            logging.error("OS error: {0}".format(err))

        except ValueError:
            logging.error("Could not convert data to an integer.")

        except Exception as error:
            logging.error(error)

        except Exception:
            sys.exit("{date} is not last date in table {table_name}".format(date=self.__param.get("date_begin"),
                                                                            table_name=self.__param.get("table_id")))
