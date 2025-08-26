from typing import Annotated
import logging
from urllib.parse import urlparse

from fastapi import Depends, HTTPException
from s3fs import S3FileSystem

from aw_connector_example.routers import router
from aw_connector_example.settings import Settings
from aw_connector_example.dependencies import get_settings, get_logger


@router.get(
    path='/health',
    summary='Проверка состояния коннектора',
    tags=['default'],
    responses={
        200: {
            'description': '',
            'content': {'application/json': {'example': {'success': True}}},
        },
        400: {},
        422: {},
        500: {
            'description': '',
            'content': {
                'application/json': {
                    'example': {
                        'detail': 'Ошибка доступа к S3 хранилищу AW BI: Could not connect to the endpoint URL: "http://192.168.161.220:8181/aw-etl'
                    }
                }
            },
        },
    },
)
async def health(
    settings: Annotated[Settings, Depends(get_settings)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
):
    """
    Проверяет работоспособность коннектора.

    Если с коннектором все хорошо и соединение к S3 хранилищу установлено успешно,
    то возвращается HTTP 200. Иначе HTTP 500.
    """
    if not settings.etl_s3_url:
        raise HTTPException(
            status_code=500, detail='Не указана переменная окружения ETL_S3_URL'
        )

    if not settings.etl_s3_bucket:
        raise HTTPException(
            status_code=500, detail='Не указана переменная окружения ETL_S3_BUCKET'
        )
    
    try:
        s3_parsed_url = urlparse(settings.etl_s3_url)

        s3fs = S3FileSystem(
            endpoint_url=f'{s3_parsed_url.scheme}://{s3_parsed_url.hostname}:{s3_parsed_url.port}',
            key=s3_parsed_url.username,
            secret=s3_parsed_url.password,
            use_ssl=s3_parsed_url.scheme == 'https',
        )

        s3fs.ls(settings.etl_s3_bucket)
    except Exception as e:
        logger.exception(
            f'Ошибка доступа к S3 хранилищу AW BI, s3 url: {settings.etl_s3_url}, bucket: {settings.etl_s3_bucket}'
        )
        raise HTTPException(
            status_code=500, detail=f'Ошибка доступа к S3 хранилищу AW BI: {e}'
        )

    return {'success': True}
