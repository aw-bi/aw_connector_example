from typing import Annotated
import logging

from fastapi import Depends, Body, HTTPException

from aw_connector_example.dto import ObjectMetaRequest, ObjectMeta
from aw_connector_example.services.repo import DataRepository, DataRepositoryError
from aw_connector_example.dependencies import get_data_repository, get_logger
from aw_connector_example.routers.data_source import router


@router.post(
    path='/object-meta',
    summary='Метаданные объекта источника',
    response_model=ObjectMeta,
    tags=['data source'],
    responses={
        200: {
            'description': '',
            'content': {
                'application/json': {
                    'example': {
                        'columns': [
                            {
                                'name': 'id',
                                'type': 'DECIMAL',
                                'simple_type': 'number',
                                'comment': None,
                            },
                            {
                                'name': 'name',
                                'type': 'VARCHAR',
                                'simple_type': 'string',
                                'comment': None,
                            }
                        ],
                        'foreign_keys': [{
                            'column_name': 'id',
                            'foreign_table_schema': 'public',
                            'foreign_table_name': 'table1',
                            'foreign_column_name': 'id'
                        }],
                    }
                }
            },
        }
    },
)
async def object_meta(
    request: Annotated[ObjectMetaRequest, Body()],
    data_repo: Annotated[DataRepository, Depends(get_data_repository)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
):
    """ 
    Возвращает метаданные объекта источника: столбцы и их типы, а также внешние связи с другими объектами источника.
    """
    logger.debug(
        f'Запрос на получение метаданных объекта /data-source/object-meta:\n{request.model_dump_json(indent=2)}'
    )
    try:
        object_meta = await data_repo.get_object_meta(
            request.data_source, request.object_name
        )
    except DataRepositoryError as e:
        logger.error(f'Не удалось получить метаданные объекта {request.object_name} из источника id={request.data_source.id}: {e}')
        raise HTTPException(status_code=400, detail=f'{e}')
    except Exception as e:
        logger.exception(
            f'Ошибка получения метаданных объекта {request.object_name} из источника id={request.data_source.id}'
        )
        raise HTTPException(status_code=500, detail=f'{e}')

    logger.debug(
        f'Ответ на запрос /data-source/object-meta:\n{object_meta.model_dump_json(indent=2)}'
    )

    return object_meta
