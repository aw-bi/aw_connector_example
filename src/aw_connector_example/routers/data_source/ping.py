from typing import Annotated
import logging

from fastapi import Depends, HTTPException, Body

from aw_connector_example.dto import PingRequest, DataSource
from aw_connector_example.services.repo import DataRepository, DataRepositoryError
from aw_connector_example.dependencies import get_data_repository, get_logger

from aw_connector_example.routers.data_source import router


@router.post(
    path='/ping',
    summary='Проверка доступности источника данных',
    tags=['data source'],
    responses={
        200: {'description': '', 'content': {'application/json': {'example': {}}}},
    },
)
async def ping(
    request: Annotated[PingRequest, Body()],
    repo: Annotated[DataRepository, Depends(get_data_repository)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
):
    """
    Проверяет источник данных на возможность подключения к нему.

    Если источник доступен, то необходимо вернуть HTTP 200 с любым (в том числе, пустым) телом ответа. 
    При наличии ошибок подключения к источнику возвращается HTTP 400/500 с указанием деталей ошибки.
    """
    logger.debug(
        f'Запрос списка объектов из источника /data-source/ping:\n{request.model_dump_json(indent=2)}'
    )

    data_source = DataSource(
        id=request.id or 0,
        type=request.type,
        params=request.params,
        extra=request.extra,
    )

    try:
        await repo.ping_data_source(data_source)
    except DataRepositoryError as e:
        logger.error(f'Не удалось подключиться к источнику: {e}')
        raise HTTPException(status_code=400, detail=f'{e}')
    except Exception as e:
        logger.exception('Ошибка подключения к источнику')
        raise HTTPException(status_code=500, detail=f'{e}')
    
    return {}
