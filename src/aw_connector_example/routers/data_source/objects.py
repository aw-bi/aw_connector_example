from typing import Annotated

import logging
import json

from fastapi import Depends, HTTPException

from aw_connector_example.dto import ObjectListRequest, DataSourceObject
from aw_connector_example.routers.data_source import router
from aw_connector_example.services.repo import DataRepository, DataRepositoryError
from aw_connector_example.dependencies import get_data_repository, get_logger


@router.post(
    path='/objects',
    response_model=list[DataSourceObject] | dict[str, list[str]],
    summary='Список объектов источника',
    tags=['data source'],
    responses={
        200: {
            'description': '',
            'content': {
                'application/json': {
                    "examples": {
                        "В плоском виде (flat: true)": {
                            'value': [
                                {'schema': 'public', 'name': 'products', 'type': 'table'}, 
                                {'schema': 'public', 'name': 'product_sales', 'type': 'table'}, 
                                {'schema': 'work', 'name': 'staff', 'type': 'table'}, 
                            ],
                        },
                        "В иерархическом виде (flat: false)": {
                            'value': {
                                'public': ['products', 'product_sales'],
                                'work': ['staff']
                            },
                        },
                    }
                }
            }
        }
    }
)
async def objects(
    request: ObjectListRequest,
    data_repo: Annotated[DataRepository, Depends(get_data_repository)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
):
    """
    Возвращает список объектов в источнике. 
    
    Каждый объект источника идентифицируется двумя значениями:
    * schema - это название каталога, в котором находится объект. Если источник не поддерживает
        такую каталогизацию, то можно в названии схемы возвращать одно и то же значение (например, public);
    * name - название объекта внутри схемы (каталога).

    Ответ возвращается в двух формах. Если в запросе указано `flat: true` (по умолчанию), то возвращается плоский список объектов:

    ```json
    [
      {"schema": "схема 1", "name": "таблица 1", "type": "table"},
      {"schema": "схема 1", "name": "таблица 2", "type": "table"},
      {"schema": "схема 2", "name": "таблица 3", "type": "table"},
      ...
    ]
    ```
        
    Если указано `flat: false`, возвращается иерархическое представление, где ключами являются названия схем,
    а значениями - названия объектов (указание типа объекта здесь не требуется):
    
    ```json
    {
      "схема 1": ["таблица 1", "таблица 2"],
      "схема 2": ["таблица 3"]
    }
    ```
    """
    logger.debug(f'Запрос списка объектов из источника /data-source/objects:\n{request.model_dump_json(indent=2)}')
    try:
        data_source_objects = await data_repo.get_objects(
            request.data_source, query_string=request.query_string
        )
    except DataRepositoryError as e:
        logger.error(f'Не удалось получить список объектов источника id={request.data_source.id}: {e}')
        raise HTTPException(status_code=400, detail=f'{e}')
    except Exception as e:
        logger.exception(f'Ошибка получения списка объектов источника id={request.data_source.id}')
        raise HTTPException(status_code=500, detail=f'{e}')

    if request.flat is None or request.flat:
        logger.debug(f'Ответ на запрос /data-source/objects:\n{json.dumps([o.model_dump(by_alias=True) for o in data_source_objects], indent=2, ensure_ascii=False)}')
        return data_source_objects
    else:
        schemas = {obj.schema_name for obj in data_source_objects}
        result = {
            schema: [
                obj.name for obj in data_source_objects if obj.schema_name == schema
            ]
            for schema in schemas
        }

        logger.debug(f'Ответ на запрос /data-source/objects:\n{json.dumps(result, indent=2, ensure_ascii=False)}')
        
        return result
