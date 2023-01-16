# -*- coding: utf-8 -*-
from src.apps.manager.bq_adapter import BqAdapter
import os, sys
import json
import logging


class ReferentialTableBuilder(BqAdapter):
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
                self.__param['query_builder'] = bool(data.get(self.__param.get("build_type"))
                                                     .get(self.__param.get("entity_name"))
                                                     .get("query_builder"))

                self.__param['write_disposition'] = str(data.get(self.__param.get("build_type"))
                                                        .get(self.__param.get("entity_name"))
                                                        .get("write_disposition"))

                self.__param['table_full'] = bool(data.get(self.__param.get("build_type"))
                                                  .get(self.__param.get("entity_name"))
                                                  .get("table_full"))

                self.__param['field_time_partitioning'] = str(data.get(self.__param.get("build_type"))
                                                              .get(self.__param.get("entity_name"))
                                                              .get("field_time_partitioning"))

                self.__param['sql_file'] = data.get(self.__param.get("build_type")).get(self.__param.get("entity_name")).get("sql_file")
                self.__param['initial_date'] = data.get(self.__param.get("build_type")).get(self.__param.get("entity_name")).get("initial_date")

                logging.info("param using : {param}".format(param=self.__param))

        except Exception as error:
            logging.error(error)

    @property
    def some_operation(self) -> None:
        try:
            # Full table
            if self.__param.get("table_full") is True and str(self.__param.get("date_begin")) == self.__param.get("initial_date"):
                self.run_query_for_full_table(
                    query=self.open_query_file(self.__param.get("sql_file").get("sql_init")),
                    project_id=self.__param.get("project_id"),
                    dataset_id=self.__param.get("dataset_id"),
                    table_id=self.__param.get("table_id"),
                    write_disposition="WRITE_APPEND" if self.__param.get("write_disposition") == 'None' else self.__param.get("write_disposition"),
                )

                logging.info(
                    "Referential {entity_name} for launching in date of {date} is DONE, this is full table".format(
                        entity_name=self.__param.get("entity_name"),
                        date=self.__param.get("date_begin"),
                    )
                )

            elif self.__param.get("table_full") is True and str(self.__param.get("date_begin")) != self.__param.get("initial_date"):
                self.run_query_for_full_table(
                    query=self.open_query_file(self.__param.get("sql_file").get("sql_current")).format(
                            date_begin=self.__param.get("date_begin"),
                            date_end=self.__param.get("date_end"),
                            dataset_id=self.__param.get("dataset_id"),
                            table_id=self.__param.get("table_id"),
                        ),
                    project_id=self.__param.get("project_id"),
                    dataset_id=self.__param.get("dataset_id"),
                    table_id=self.__param.get("table_id"),
                    write_disposition="WRITE_APPEND" if self.__param.get("write_disposition") == 'None' else self.__param.get("write_disposition"),
                )

                logging.info(
                    "Referential {entity_name} for launching in date of {date} is DONE, this is full table".format(
                        entity_name=self.__param.get("entity_name"),
                        date=self.__param.get("date_begin"),
                    )
                )

            # partitioned Table
            elif self.__param.get("table_full") is False and str(self.__param.get("date_begin")) == self.__param.get("initial_date"):
                self.run_query_for_partition_table(
                    query=self.open_query_file(self.__param.get("sql_file").get("sql_init")),
                    project_id=self.__param.get("project_id"),
                    dataset_id=self.__param.get("dataset_id"),
                    table_id=self.__param.get("table_id"),
                    field_time_partitioning="date" if self.__param.get("field_time_partitioning") == 'None' else self.__param.get("field_time_partitioning"),
                    write_disposition="WRITE_APPEND" if self.__param.get("write_disposition") == 'None' else self.__param.get("write_disposition"),
                )

                logging.info(
                    "Referential {entity_name} for launching in date of {date} is DONE, this is partitioned table".format(
                        entity_name=self.__param.get("entity_name"),
                        date=self.__param.get("date_begin"),
                    )
                )

            elif self.__param.get("table_full") is False and str(self.__param.get("date_begin")) != self.__param.get("initial_date"):
                self.run_query_for_partition_table(
                    query=self.open_query_file(self.__param.get("sql_file").get("sql_current")).format(
                            date_begin=self.__param.get("date_begin"),
                            date_end=self.__param.get("date_end"),
                            dataset_id=self.__param.get("dataset_id"),
                            table_id=self.__param.get("table_id"),
                        ),
                    project_id=self.__param.get("project_id"),
                    dataset_id=self.__param.get("dataset_id"),
                    table_id=self.__param.get("table_id"),
                    field_time_partitioning="date" if self.__param.get("field_time_partitioning") == 'None' else self.__param.get("field_time_partitioning"),
                    write_disposition="WRITE_APPEND" if self.__param.get("write_disposition") == 'None' else self.__param.get("write_disposition"),
                )

                logging.info(
                    "Referential {entity_name} for launching in date of {date} is DONE, this is partitioned table".format(
                        entity_name=self.__param.get("entity_name"),
                        date=self.__param.get("date_begin"),
                    )
                )

        except (FileNotFoundError, IOError):
            logging.error("Wrong file or file path")

        except OSError as err:
            logging.error("OS error: {0}".format(err))

        except ValueError:
            logging.error(ValueError)

        except Exception:
            sys.exit("error found when you call {value}".format(value=self.__param.get('query_builder')))

    @staticmethod
    def get_queries_file() -> str:
        """

        Returns:

        """
        pass
