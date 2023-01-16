# -*- coding: utf-8 -*-
import fileinput
from abc import ABC, abstractmethod
from abc import abstractmethod
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import logging
import os
import json

load_dotenv()


class BqAdapter(ABC):
    def __init__(self) -> None:
        """
        BqAdapter : Abstract (parent) class that groups the custom BigQuery functions used by the daughter classes of each insight
        """
        self.__client_bq = self.build_bigquery_client()

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

    def get_param_config(self):
        pass

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

    def run_query_for_full_table(
        self,
        query,
        project_id: str = "",
        dataset_id: str = "",
        table_id: str = "",
        write_disposition: str = "WRITE_APPEND",
        location: str = "EU",
    ) -> None:
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
            dataset_id_full = "{}.{}".format(project_id, dataset_id)
            dataset = bigquery.Dataset(dataset_id_full)
            dataset.location = location
            table_ref = dataset.table(table_id)
            job_config = bigquery.QueryJobConfig()
            job_config.destination = table_ref
            job_config.use_legacy_sql = False
            job_config.allow_large_results = True
            job_config.write_disposition = write_disposition
            self.__client_bq.query(query, job_config=job_config).result()
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

    def run_query_for_partition_table(
        self,
        query: str = "",
        project_id: str = "",
        dataset_id: str = "",
        table_id: str = "",
        field_time_partitioning: str = "partition_date",
        write_disposition: str = "WRITE_APPEND",
        location: str = "EU",
    ) -> None:
        """
        run_query_for_full_table : Function that allows to insert data in a partition table from a SQL usecase
        Args:
            query: SQL usecase for BigQuery
            project_id (str): project id value using in GCP
            dataset_id (str): dataset name value using in BigQuery
            table_id (str): table name value using in BigQuery
            field_time_partitioning (str): Field using for partition table
            write_disposition (str) : Describes whether a mutation to a table should overwrite , append or other options
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
            time_partitioning = bigquery.table.TimePartitioning(
                field=field_time_partitioning
            )
            dataset_id_full = "{}.{}".format(project_id, dataset_id)
            dataset = bigquery.Dataset(dataset_id_full)
            dataset.location = location
            table_ref = dataset.table(table_id)
            job_config = bigquery.QueryJobConfig()
            job_config.destination = table_ref
            job_config.time_partitioning = time_partitioning
            job_config.use_legacy_sql = False
            job_config.allow_large_results = True
            job_config.write_disposition = write_disposition
            query_job = self.__client_bq.query(query, job_config=job_config)
            query_job.result()
        except Exception as e:
            logging.warning(
                "crash run usecase for partition table with error : {e}".format(e=e)
            )
