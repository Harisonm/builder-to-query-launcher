# -*- coding: utf-8 -*-
import fileinput
from abc import ABC, abstractmethod
from abc import abstractmethod
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import logging
import pandas as pd
import os

load_dotenv()


class UtilsMediator(ABC):
    def __init__(self) -> None:
        """
        BqAdapter : Abstract (parent) class that groups the custom BigQuery functions used by the daughter classes of each insight
        """
        self.__client_bq = self.build_bigquery_client()

    def get_param_config(self):
        pass

    def some_operation_util(self):
        pass

    @staticmethod
    def build_bigquery_client():
        """
        build_bigquery_client: Static function that allows to build the service account management for the different BigQuery applications
        Returns:
            : bigquery.Client
        """
        try:
            credentials = service_account.Credentials.from_service_account_file(
                filename=os.getenv("service_account")
            )
            return bigquery.Client(
                credentials=credentials, project=credentials.project_id, location="EU"
            )
        except Exception as e:
            logging.warning("Crash build bigquery Client: {e}".format(e=e))

    @staticmethod
    def open_query_file(sql_file: fileinput) -> str:
        """
        open_query_file : Function to call .sql file
        Args:
            sql_file (file): file_name.sql, the file that stores your request

        Returns:
            str: BigQuery usecase format string
        """
        try:

            return " ".join(open(sql_file, "r").readlines())
        except Exception as e:
            logging.warning("Crash open usecase file {e}".format(e=e))

    def run_query(self, query, write_disposition: str = "WRITE_EMPTY") -> pd.DataFrame:
        """
        run_query_for_full_table : Function that allows to insert data in a full table from a SQL usecase
        Args:
            query: SQL usecase for BigQuery
            project_id (str): project id value using in GCP
            dataset_id (str): dataset name value using in BigQuery
            table_id (str): table name value using in BigQuery
            write_disposition (str): Describes whether a mutation to a table should overwrite , append or other options
                options :
                WRITE_DISPOSITION_UNSPECIFIED	Unknown.
                WRITE_EMPTY	This job should only be writing to empty tables.
                WRITE_TRUNCATE	This job will truncate table data and write from the beginning.
                WRITE_APPEND	This job will append to a table.
            location: Value of localisation of your Dataset (EU,US ..)

        Returns:
            None
        """
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = False
            job_config.allow_large_results = True
            job_config.write_disposition = write_disposition
            return self.__client_bq.query(query, job_config=job_config).to_dataframe()
        except Exception as e:
            logging.info(e)

    @staticmethod
    @abstractmethod
    def get_queries_file():
        """
        Function Abstract for child of BqAdapter Class.
        Returns:
            None
        """
        pass
