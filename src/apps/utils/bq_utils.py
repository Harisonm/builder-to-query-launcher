# -*- coding: utf-8 -*-
from google.cloud import bigquery
from google.oauth2 import service_account
from src.apps.utils.mediator.utils_mediator import UtilsMediator
import logging
import sys
import os


class BqUtils(UtilsMediator):
    def __init__(self, param):
        """

        Args:
            param:
        """
        super().__init__()
        self.__param = param

    @property
    def some_operation_util(self):
        """
        Function using to do operation
        Returns:

        """
        getattr(self, self.__param.get("action_utils"), "func_not_found")

    def func_not_found(self):
        """
        Function using to print error in logging if function search is not found
        Returns:

        """
        logging.error("No Function " + self.__param.get("action_utils") + " Found!")

    @staticmethod
    def build_bigquery_client():
        """

        Returns:

        """
        credentials = service_account.Credentials.from_service_account_file(
            filename=os.getenv("service_account")
        )
        return bigquery.Client(
            credentials=credentials, project=credentials.project_id, location="EU"
        )

    @property
    def delete_dataset(self, location="EU"):
        """

        Args:
            location:

        Returns:

        """
        self.__client_bq = self.build_bigquery_client()
        dataset_id_full = "{}.{}".format(
            self.__param.get("project_id"), self.__param.get("dataset_id")
        )
        dataset = bigquery.Dataset(dataset_id_full)
        dataset.location = location
        self.__client_bq.delete_dataset(
            self.__param.get("dataset_id"), delete_contents=True, not_found_ok=True
        )
        logging.info(
            "Deleted '{dataset}' is delete".format(
                dataset=self.__param.get("dataset_id")
            )
        )

    @property
    def create_dataset(self, location="EU"):
        """

        Args:
            location:

        Returns:

        """
        try:
            self.__client_bq = self.build_bigquery_client()
            dataset_id_full = "{}.{}".format(
                self.__param.get("project_id"), self.__param.get("dataset_id")
            )
            dataset = bigquery.Dataset(dataset_id_full)
            dataset.location = location
            self.__client_bq.create_dataset(dataset)
            logging.info(
                "Dataset '{dataset}' is created".format(
                    dataset=self.__param.get("dataset_id")
                )
            )
        except OSError as err:
            logging.error("OS error: {0}".format(err))
        except ValueError:
            logging.error(ValueError)
        except Exception as e:
            tb = sys.exc_info()
            logging.error(e.with_traceback(tb[2]))

    @property
    def create_table(self):
        pass

    @property
    def delete_table(self):
        pass

    @staticmethod
    def get_queries_file():
        """
        Function Abstract for child of BqAdapter Class.
        Returns:
            None
        """
        pass
