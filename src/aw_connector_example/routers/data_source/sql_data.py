from typing import Annotated
import logging

from fastapi import Depends, HTTPException, Body

from aw_connector_example.dto import SqlDataRequest, ObjectData
from aw_connector_example.services.repo import DataRepository, DataRepositoryError
from aw_connector_example.dependencies import get_data_repository, get_logger
from aw_connector_example.routers.data_source import router


@router.post(
    path='/sql-object-data',
    summary='Данные SQL запросу к источнику (предпросмотр)',
    tags=['data source'],
    response_model=ObjectData,
)
async def sql_data(
    request: Annotated[SqlDataRequest, Body()],
    repo: Annotated[DataRepository, Depends(get_data_repository)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
):
    """ 
    Предварительный просмотр (preview) данных SQL-запроса к источнику.
    """
    logger.debug(
        f'Запрос на получение данных SQL-запроса /data-source/sql-object-data:\n{request.model_dump_json(indent=2)}'
    )

    limit, offset = None, None
    if request.page is not None and request.page_size is not None:
        limit, offset = request.page_size, (request.page - 1) * request.page_size

    try:
        rows = await repo.get_sql_data(
            data_source=request.data_source,
            sql_text=request.sql_text,
            limit=limit,
            offset=offset,
        )
    except DataRepositoryError as e:
        logger.error(
            f'Не удалось получить данные SQL запроса {request.sql_text} из источника id={request.data_source.id}: {e}'
        )
        raise HTTPException(status_code=400, detail=f'{e}')
    except Exception as e:
        logger.exception(
            f'Ошибка получения данных SQL запроса {request.sql_text} из источника id={request.data_source.id}'
        )
        raise HTTPException(status_code=500, detail=f'{e}')

    object_data = ObjectData(data=rows)

    logger.debug(
        f'Ответ на запрос /data-source/sql-object-data:\n{object_data.model_dump_json(indent=2)}'
    )

    return object_data
