# -*- coding: utf-8 -*-
import json
import logging
import os
from datetime import timedelta, datetime

from tqdm import tqdm
from src.apps.model.referential_table_builder import ReferentialTableBuilder
from src.apps.utils.mediator.utils_mediator import UtilsMediator


class DataCatchUp(UtilsMediator):
    def __init__(self, param):
        """

        Args:
            param:
        """
        super().__init__()
        self.__param = param

    @property
    def some_operation_util(self) -> int:

        try:
            date_begin = datetime.strptime(
                self.__param.get("date_begin"), "%Y-%m-%d"
            ).date()
            date_end = datetime.strptime(
                self.__param.get("date_end"), "%Y-%m-%d"
            ).date()
            param = self.__param
            delta = date_end - date_begin

            for i in tqdm(range(delta.days)):
                date_current = date_begin + timedelta(days=i)
                param.update({"date_begin": date_current})
                param.update({"date_end": date_current})
                param.update({"build_type": self.__param.get("use_case_target")})
                param.update({"entity_name": self.__param.get("action_utils")})
                getattr(globals()["ReferentialTableBuilder"](param=param), "some_operation")

        except (FileNotFoundError, IOError):
            logging.error("Wrong file or file path")

        except OSError as err:
            logging.error("OS error: {0}".format(err))

        except ValueError:
            logging.error(ValueError)
            raise

    def get_queries_file(self):
        pass
