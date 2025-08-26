from typing import Annotated

import logging
from fastapi import Depends, Body, HTTPException

from aw_connector_example.dto import ObjectDataRequest, ObjectData
from aw_connector_example.services.repo import DataRepository, DataRepositoryError
from aw_connector_example.dependencies import get_data_repository, get_logger
from aw_connector_example.routers.data_source import router


@router.post(
    path='/object-data',
    response_model=ObjectData,
    tags=['data source'],
    summary='Данные объекта источника (предпросмотр)',
)
async def object_data(
    request: Annotated[ObjectDataRequest, Body()],
    data_repo: Annotated[DataRepository, Depends(get_data_repository)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
):
    """
    Предварительный просмотр (preview) данных объекта источника.
    """
    logger.debug(
        f'Запрос на получение данных объекта /data-source/object-data:\n{request.model_dump_json(indent=2)}'
    )

    limit, offset = None, None
    if request.page is not None and request.page_size is not None:
        limit, offset = request.page_size, (request.page - 1) * request.page_size

    try:
        rows = await data_repo.get_object_data(
            request.data_source, request.object_name, limit=limit, offset=offset
        )
    except DataRepositoryError as e:
        logger.error(
            f'Не удалось получить данные объекта {request.object_name} из источника id={request.data_source.id}: {e}'
        )
        raise HTTPException(status_code=400, detail=f'{e}')
    except Exception as e:
        logger.exception(
            f'Ошибка получения данных объекта {request.object_name} из источника id={request.data_source.id}'
        )
        raise HTTPException(status_code=500, detail=f'{e}')

    object_data = ObjectData(data=rows)

    logger.debug(
        f'Ответ на запрос /data-source/object-data:\n{object_data.model_dump_json(indent=2)}'
    )

    return object_data
