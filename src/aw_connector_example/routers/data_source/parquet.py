from typing import Annotated
import logging
import uuid
import asyncio
from urllib.parse import urlparse

from fastapi import Depends, Body, Path, HTTPException, BackgroundTasks, Response
from s3fs import S3FileSystem

from aw_connector_example.dto import ParquetRequest
from aw_connector_example.services.repo import DataRepository, DataRepositoryError
from aw_connector_example.services.parquet import ParquetService
from aw_connector_example.services.parquet_queue import ParquetQueue
from aw_connector_example.settings import Settings
from aw_connector_example.dependencies import (
    get_data_repository,
    get_parquet_service,
    get_parquet_queue_service,
    get_logger,
    get_settings,
)
from aw_connector_example.routers.data_source import router


@router.post(
    path='/parquet',
    summary='Выгрузка данных в parquet',
    tags=['data source'],
    responses={
        200: {
            'description': 'Данные успешно выгружены',
            'content': {'application/json': {'example': 'null'}},
        },
        202: {
            'description': 'Выгрузка данных начата. Повторить запрос через Retry-After секунд URL из Location '
            '(Retry-After и Location указываются в заголовках ответа)',
            'content': {'application/json': {'example': 'null'}},
        },
    },
)
async def parquet(
    request: Annotated[ParquetRequest, Body()],
    data_repo: Annotated[DataRepository, Depends(get_data_repository)],
    parquet_service: Annotated[ParquetService, Depends(get_parquet_service)],
    parquet_queue: Annotated[ParquetQueue, Depends(get_parquet_queue_service)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
    settings: Annotated[Settings, Depends(get_settings)],
    background_tasks: BackgroundTasks,
    response: Response,
):
    """
    Выгружает данные объекта источника (или SQL-запроса к источнику) в parquet.

    В этом примере пользовательского коннектора реализованы оба способа взаимодействия с API
    по выгрузке данных в parquet:

    * Синхронный режим. Отправить запроса получает ответ по окончанию выгрузки
      (HTTP 200 при успешном завершении, либо HTTP 400+ при наличии ошибок);
    * Асинхронный режим. Отправитель запроса после начала выгрузки получает ответ со статусом HTTP 202.
      При этом, в заголовках ответа указыватся:
        * Location: URL, по которому нужно проверить состояние выгрузки в следующий раз;
        * Retry-After: cделать запрос по URL из Location через столько секунд.
    """
    logger.debug(
        f'Запрос на выгрузку данных в parquet /data-source/parquet:\n{request.model_dump_json(indent=2)}'
    )

    async def export_to_parquet():
        if request.object.type == 'sql':
            if not request.object.query_text:
                raise Exception(
                    'Для объекта с типом sql не указан текст sql-запроса (параметр query_text)'
                )

            object_data = await data_repo.get_sql_data(
                data_source=request.object.data_source,
                sql_text=request.object.query_text,
                offset=0,
                limit=request.limit,
                filters=request.filters,
            )
        else:
            object_data = await data_repo.get_object_data(
                data_source=request.object.data_source,
                object_name=request.object.name,
                offset=0,
                limit=request.limit,
                filters=request.filters,
            )

        parquet_table = await parquet_service.read_table(object_data)

        if request.object.fields:
            # указан явный список столбцов, оставляем в таблице для выгрузки только их
            parquet_table = parquet_table.drop_columns(
                {c for c in parquet_table.column_names}
                - {f.name for f in request.object.fields}
            )

        if request.folder.startswith('s3://'):
            # Выгрузка в S3
            s3_parsed_url = urlparse(settings.etl_s3_url)

            s3fs = S3FileSystem(
                endpoint_url=f'{s3_parsed_url.scheme}://{s3_parsed_url.hostname}:{s3_parsed_url.port}',
                key=s3_parsed_url.username,
                secret=s3_parsed_url.password,
                use_ssl=s3_parsed_url.scheme == 'https',
            )

            s3_path = settings.etl_s3_bucket + '/' + request.folder.lstrip('s3://')

            try:
                await parquet_service.write_table_s3(
                    table=parquet_table,
                    s3_path=s3_path,
                    s3fs=s3fs,
                )
            except Exception as e:
                logger.exception(
                    f'Ошибка выгрузки данных в S3 для {request.object.name} из источника id={request.object.data_source.id}'
                )
                raise Exception(
                    f'Не удалось выгрузить данные для {request.object.name}: {e}'
                )
        else:
            # Выгрузка в файловую систему
            fs_path = f'/file_storage/{request.folder}'

            try:
                await parquet_service.write_table_fs(
                    table=parquet_table,
                    fs_path=fs_path,
                )
            except Exception as e:
                logger.exception(
                    f'Ошибка выгрузки данных в файловую систему для {request.object.name} из источника id={request.object.data_source.id}'
                )
                raise Exception(
                    f'Не удалось выгрузить данные для {request.object.name}: {e}'
                )

    async def export_parquet_background(task_id: str):
        """ """
        await asyncio.sleep(2)
        try:
            await export_to_parquet()
        except Exception as e:
            await parquet_queue.error_task(task_id, error=f'{e}')
        else:
            await parquet_queue.finish_task(task_id)

    if request.object.data_source.extra and 'async' in request.object.data_source.extra:
        # запускаем задачу выгрузки данных в фоновом режиме
        task_id = uuid.uuid4().hex

        await parquet_queue.start_task(task_id)
        background_tasks.add_task(export_parquet_background, task_id=task_id)

        response.headers['Location'] = f'data-source/parquet/queue/{task_id}'
        response.headers['Retry-After'] = '0.5'
        response.status_code = 202

    else:
        try:
            await export_to_parquet()
        except Exception as e:
            logger.exception('Ошибка выгрузки данных в parquet')
            raise HTTPException(status_code=500, detail=f'{e}')

    return


@router.get(
    path='/parquet/queue/{task_id}',
    summary='Состояние задачи выгрузку данных в parquet (при асинхронной выгрузке)',
    tags=['async'],
    responses={
        200: {'description': '', 'content': {'application/json': {'example': 'null'}}},
    },
)
async def parquet_queue(
    task_id: Annotated[str, Path()],
    parquet_queue: Annotated[ParquetQueue, Depends(get_parquet_queue_service)],
    logger: Annotated[logging.Logger, Depends(get_logger)],
    response: Response,
):
    """
    Проверяет состяние выгрузки данных в parquet при асинхронном способе взаимодействия.

    Если выгрузка данных ещё не звершено не готова, то возвращается HTTP 202 и в заголовках ответа указывается:
    * Location: URL, по которому нужно проверить состояние выгрузки в следующий раз;
    * Retry-After: cделать запрос по URL из Location через столько секунд.

    Если выгрузка завершена, то возвращается HTTP 200 с пустым телом ответа.
    """
    task_status = await parquet_queue.get_task_status(task_id)

    if task_status == 'started':
        response.headers['Location'] = f'data-source/parquet/queue/{task_id}'
        response.headers['Retry-After'] = '0.5'
        response.status_code = 202
        return

    if task_status.startswith('error'):
        raise HTTPException(status_code=500, detail=task_status)

    if task_status == 'finished':
        # почистим за собой
        try:
            await parquet_queue.clear_task(task_id)
        except Exception:
            logger.exception(f'Не удалось очистить информацию по задаче {task_id}')

    return
