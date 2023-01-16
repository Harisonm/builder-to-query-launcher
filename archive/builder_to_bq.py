from fastapi import FastAPI, Response
import logging
from typing import List, Optional
from datetime import datetime, timedelta
from src.apps.manager.entity_factory import BqManager
from pydantic import BaseModel, Field
import os

app = FastAPI()


class ArgBuilder(BaseModel):
    build_type: str = Field(
        None,
        description="Name of build using : datamart , a_referential_table or utils",
    )
    entity_name: str = Field(
        None,
        description="Name of class for call Insight factory Class : insights_client, InsightsVente, InsightsStock, referential_product, InsightsEcommerce ",
    )
    date_begin: Optional[str] = Field(None, description="Date of usecase need to begin")
    date_end: Optional[str] = Field(None, description="Date of usecase need to end")
    path_destination_bq: str = Field(
        None,
        description="Path of destination usecase to write in BigQuery : project_id.dataset_id.table_id",
    )


@app.post("/builder_to_bq", status_code=201)
def builder_to_bq(item: ArgBuilder, response: Response):
    """
    Args:
        build_type:
        item: build_type
        response:

    Returns:
    """
    build_type = item.build_type
    entity_name = item.entity_name
    date_begin = item.date_begin
    date_end = item.date_end
    path_destination_bq = item.path_destination_bq

    BqManager(
        build_type=build_type,
        entity_name=entity_name,
        date_begin=date_begin,
        date_end=date_end,
        path_destination_bq=path_destination_bq,
    ).call_entity()
    response.status_code = 201
    return "build to bq is finish"
