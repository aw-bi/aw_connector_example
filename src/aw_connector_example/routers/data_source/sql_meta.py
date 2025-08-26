from typing import Annotated
import logging

from fastapi import Depends, HTTPException, Body

from aw_connector_example.dto import SqlMetaRequest, ObjectMeta
from aw_connector_example.services.repo import DataRepository, DataRepositoryError
from aw_connector_example.dependencies import get_data_repository, get_logger
from aw_connector_example.routers.data_source import router


@router.post(
    path='/sql-meta',
    summary='Метаданные SQL запросу к источнику',
    tags=['data source'],
    response_model=ObjectMeta,
)
async def sql_meta(
    request: Annotated[SqlMetaRequest, Body()],
    repo: Annotated[DataRepository, Depends(get_data_repository)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
):
    """
    Возвращает метаданные результата выполнения SQL запроса к объектам источника: список столбцов и их типы.
    """
    logger.debug(
        f'Запрос метаданных SQL запроса /data-source/sql-meta:\n{request.model_dump_json(indent=2)}'
    )

    try:
        sql_meta = await repo.get_sql_meta(
            data_source=request.data_source, sql_text=request.sql_text
        )
    except DataRepositoryError as e:
        logger.error(
            f'Не удалось получить метаданные SQL запроса {request.sql_text} из источника id={request.data_source.id}: {e}'
        )
        raise HTTPException(status_code=400, detail=f'{e}')
    except Exception as e:
        logger.exception(
            f'Ошибка получения метаданных SQL запроса {request.sql_text} из источника id={request.data_source.id}'
        )
        raise HTTPException(status_code=500, detail=f'{e}')

    logger.debug(
        f'Ответ на запрос /data-source/sql-meta:\n{sql_meta.model_dump_json(indent=2)}'
    )

    return sql_meta
