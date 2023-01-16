# -*- coding: utf-8 -*-
from src.apps.manager.bq_adapter import BqAdapter
from src.apps.model.children.query_builder import QueryBuilder
from tqdm import tqdm
import logging
import os, sys
import json


class DatamartBuilder(BqAdapter):
    def __init__(self, param):
        """
        Classe using to build Table in BigQuery for @FnacDarty
        Args:
            param (dict): dictionary including the parameters required to call the GCP library
                          'build_type': build_type,
                          'entity_name': entity_name,
                          'date_begin': date_begin,
                          'date_end': date_end,
                          'project_id': project_id,
                          'dataset_id': dataset_id,
                          'table_id': table_id
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

                logging.info("param using : {param}".format(param=self.__param))

        except Exception as error:
            logging.error(error)

    @property
    def some_operation(self) -> None:
        """
        Function that calls BigQuery usecase to do data transformation and then re-insert into BigQuery
        Returns:
            None
        """
        try:
            if self.__param['query_builder']:
                QueryBuilder(param=self.__param).some_operation()

            else:
                for v in self.__param['sql_file'].values():
                    query = self.open_query_file(v)
                    if self.__param.get("table_full") is False:
                        self.run_query_for_partition_table(
                            query=query.format(
                                date_begin=self.__param.get("date_begin"),
                                date_end=self.__param.get("date_end"),
                            ),
                            project_id=self.__param.get("project_id"),
                            dataset_id=self.__param.get("dataset_id"),
                            table_id=self.__param.get("table_id"),
                            field_time_partitioning="date" if self.__param.get("field_time_partitioning") == 'None' else self.__param.get("field_time_partitioning"),
                            write_disposition="WRITE_APPEND" if self.__param.get("write_disposition") == 'None' else self.__param.get("write_disposition"),
                        )
                    else:
                        self.run_query_for_full_table(
                            query=query.format(
                                date_begin=self.__param.get("date_begin"),
                                date_end=self.__param.get("date_end"),
                            ),
                            project_id=self.__param.get("project_id"),
                            dataset_id=self.__param.get("dataset_id"),
                            table_id=self.__param.get("table_id"),
                            write_disposition="WRITE_APPEND" if self.__param.get("write_disposition") == 'None' else self.__param.get("write_disposition"),
                        )

                logging.info(
                    "Insights {entity_name} for {date} is DONE".format(
                        entity_name=self.__param.get("entity_name"),
                        date=self.__param.get("date_begin"),
                    )
                )
        except OSError as err:
            logging.error("OS error: {0}".format(err))
        except ValueError:
            logging.error("Values error {0}".format(ValueError))
        except Exception:
            sys.exit("error found when you call : {value}".format(value=self.__param))

    @staticmethod
    def get_queries_file() -> str:
        """

        Returns:

        """
        pass
