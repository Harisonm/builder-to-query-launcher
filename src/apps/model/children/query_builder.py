# -*- coding: utf-8 -*-
from resources.usecase.b_datamarts.insights_clients.darty.config.config import (
    get_all_values_darty,
    get_kpi_group_darty,
)
from resources.usecase.b_datamarts.insights_clients.fnac.config.config import (
    get_all_values_fnac,
    get_kpi_group_fnac,
)
from src.apps.manager.bq_adapter import BqAdapter
from tqdm import tqdm
import logging , sys


class QueryBuilder(BqAdapter):
    def __init__(self, param):
        """
        Classe using to build Datamart Insights Client for @FnacDarty
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
        self.__tab_fnac, self.__tab_darty = [], []
        self.__kpi_group_fnac, self.__kpi_group_darty = (
            get_kpi_group_fnac(),
            get_kpi_group_darty(),
        )

    @property
    def some_operation(self) -> None:
        """
        Function that calls BigQuery usecase to do data transformation and then re-insert into BigQuery
        Returns:
            None
        """
        try:
            self.build_tab()

            query_darty_init_niv_1_2_3 = self.open_query_file(
                self.__param.get("sql_file").get("darty_niv1_2_3_sql")
            )
            query_darty_key_value_niv_4 = self.open_query_file(
                self.__param.get("sql_file").get("darty_key_value")
            )
            query_darty_init_niv_4 = self.open_query_file(
                self.__param.get("sql_file").get("darty_niv4_sql"))

            query_fnac_init_niv_1_2_3 = self.open_query_file(
                self.__param.get("sql_file").get("fnac_niv1_2_3_sql")
            )
            query_fnac_key_value_niv_4 = self.open_query_file(
                self.__param.get("sql_file").get("fnac_key_value")
            )
            query_fnac_init_niv_4 = self.open_query_file(
                self.__param.get("sql_file").get("fnac_niv4_sql"))

            # Insert into BQ for Darty niv 1,2,3
            self.create_request_simple_from_bq(
                request=query_darty_init_niv_1_2_3,
                tab_field=self.__tab_darty[0],
                kpi_group=self.__kpi_group_darty,
                project_id_dest=self.__param.get("project_id"),
                dataset_id_dest=self.__param.get("dataset_id"),
                table_id_dest=self.__param.get("table_id"),
                field_time_partitioning_dest="date" if self.__param.get("field_time_partitioning") == 'None' else self.__param.get("field_time_partitioning"),
            )

            # Insert into BQ for Fnac niv 1,2,3
            self.create_request_simple_from_bq(
                request=query_fnac_init_niv_1_2_3,
                tab_field=self.__tab_fnac[0],
                kpi_group=self.__kpi_group_fnac,
                project_id_dest=self.__param.get("project_id"),
                dataset_id_dest=self.__param.get("dataset_id"),
                table_id_dest=self.__param.get("table_id"),
                field_time_partitioning_dest="date" if self.__param.get("field_time_partitioning") == 'None' else self.__param.get("field_time_partitioning"),
            )

            # Insert into BQ for Darty niv 4
            self.create_request_with_joint_from_bq(
                request=query_darty_key_value_niv_4,
                query_initial=query_darty_init_niv_4,
                enseigne="DARTY",
                tab_field=self.__tab_darty[0],
                kpi_group=self.__kpi_group_darty,
                project_id_dest=self.__param.get("project_id"),
                dataset_id_dest=self.__param.get("dataset_id"),
                table_id_dest=self.__param.get("table_id"),
                field_time_partitioning_dest="date" if self.__param.get("field_time_partitioning") == 'None' else self.__param.get("field_time_partitioning"),
            )

            # Insert into BQ for Fnac 4
            self.create_request_with_joint_from_bq(
                request=query_fnac_key_value_niv_4,
                query_initial=query_fnac_init_niv_4,
                enseigne="FNAC",
                tab_field=self.__tab_fnac[0],
                kpi_group=self.__kpi_group_fnac,
                project_id_dest=self.__param.get("project_id"),
                dataset_id_dest=self.__param.get("dataset_id"),
                table_id_dest=self.__param.get("table_id"),
                field_time_partitioning_dest="date" if self.__param.get("field_time_partitioning") == 'None' else self.__param.get("field_time_partitioning"),
            )

            logging.info(
                "Insights Client global for {date} is DONE".format(
                    date=self.__param.get("date_begin")
                )
            )
            sys.exit(0)

        except OSError as err:
            logging.error("OS error: {0}".format(err))
        except ValueError:
            logging.error("Values error {0}".format(ValueError))
            sys.exit(1)

    def build_tab(self):
        """
        Build data table with key/value to insights client
        Returns:

        """
        self.__tab_fnac.extend([get_all_values_fnac()])
        self.__tab_darty.extend([get_all_values_darty()])

    def get_queries_file(self):
        """
        Function to get file .SQL for Fnac & Darty
        Returns:

        """
        pass

    def create_request_with_joint_from_bq(
        self,
        request,
        query_initial,
        enseigne,
        tab_field,
        kpi_group,
        project_id_dest,
        dataset_id_dest,
        table_id_dest,
        field_time_partitioning_dest,
        write_disposition="WRITE_APPEND",
    ):
        """
        Function to create Datamart for insights Client level 4
        Args:
            request:
            query_initial:
            enseigne:
            tab_field:
            kpi_group:
            project_id_dest:
            dataset_id_dest:
            table_id_dest:
            field_time_partitioning_dest:
            write_disposition:

        Returns:

        """
        enseigne_low = enseigne.lower()
        enseigne_up = enseigne.upper()
        try:
            for k in tqdm(range(0, len(kpi_group))):
                re = query_initial.format(
                    enseigne_low=enseigne_low,
                    enseigne_up=enseigne_up,
                    date_begin=self.__param.get("date_begin"),
                    date_end=self.__param.get("date_end"),
                )
                kpi_group_name = kpi_group[k]
                for v in tab_field[k][kpi_group_name]:
                    re += " "
                    re += request.format(field_name=v, kpi_group_name=kpi_group_name)

                re = self.remove_last_line_from_string(re)

                # Call function insert data in table
                self.run_query_for_partition_table(
                    query=re,
                    project_id=project_id_dest,
                    dataset_id=dataset_id_dest,
                    table_id=table_id_dest,
                    field_time_partitioning=field_time_partitioning_dest,
                    write_disposition=write_disposition,
                )

            logging.info(
                "Insights Client niv4 key-value for {date}  is DONE".format(
                    date=self.__param.get("date_begin")
                )
            )

        except Exception as e:
            logging.error(
                "Error in create_request_with_joint_from_bq function, description : {e}".format(
                    e=e
                )
            )

    def create_request_simple_from_bq(
        self,
        request,
        tab_field,
        kpi_group,
        project_id_dest,
        dataset_id_dest,
        table_id_dest,
        field_time_partitioning_dest,
        write_disposition="WRITE_APPEND",
    ):
        """
        Function to create Datamart for insights Client level 1,2,3
        Args:
            request:
            tab_field:
            kpi_group:
            project_id_dest:
            dataset_id_dest:
            table_id_dest:
            field_time_partitioning_dest:
            write_disposition:

        Returns:

        """
        try:
            for i in tqdm(range(1, 4)):
                niv = "niv" + str(i)
                for k in tqdm(range(0, len(kpi_group))):
                    re = ""
                    kpi_group_name = kpi_group[k]
                    for v in tab_field[k][kpi_group_name]:
                        re += " "
                        re += request.format(
                            field_name=v,
                            kpi_group_name=kpi_group_name,
                            niv=niv,
                            date_begin=self.__param.get("date_begin"),
                            date_end=self.__param.get("date_end"),
                        )

                    re = self.remove_last_line_from_string(re)

                    # Call function insert data in table
                    self.run_query_for_partition_table(
                        query=re,
                        project_id=project_id_dest,
                        dataset_id=dataset_id_dest,
                        table_id=table_id_dest,
                        field_time_partitioning=field_time_partitioning_dest,
                        write_disposition=write_disposition,
                    )

                    logging.info(
                        "Insights Client niv 1,2,3 key-value for {date}  is DONE".format(
                            date=self.__param.get("date_begin")
                        )
                    )

        except Exception as e:
            logging.error(
                "Error in create_request_simple_from_bq function, description : {e}".format(
                    e=e
                )
            )

    @staticmethod
    def remove_last_line_from_string(s):
        """

        Args:
            s:

        Returns:

        """
        try:
            return s[: s.rfind("\n")]
        except Exception as e:
            logging.error(
                "Error in remove_last_line_from_string function, description : {e}".format(
                    e=e
                )
            )
