from typing import Annotated
import os
from functools import lru_cache
import logging
from pathlib import Path

from fastapi import Depends

from aw_connector_example.services.repo import DataRepository
from aw_connector_example.services.parquet import ParquetService
from aw_connector_example.services.parquet_queue import ParquetQueue
from aw_connector_example.settings import Settings


def get_logger() -> logging.Logger:
    uvicorn_logger = logging.getLogger('uvicorn')
    log_level = os.getenv('LOG_LEVEL')
    if log_level:
        uvicorn_logger.setLevel(log_level.upper())
    return uvicorn_logger


def get_data_root_folder() -> Path:
    """
    Возвращает путь к папке с данными
    """
    return Path(__file__).parent / 'data'


def get_data_repository(
    data_root_folder: Annotated[Path, Depends(get_data_root_folder)],
) -> DataRepository:
    """
    Возвращает репозиторий для доступа к данным
    """
    return DataRepository(data_root_folder)


def get_parquet_service():
    """
    Возвращает сервис для работы с parquet-таблицами
    """
    return ParquetService()


def get_parquet_queue_service() -> ParquetQueue:
    """ """
    return ParquetQueue(root=Path(__file__).parent / '.queue')


@lru_cache
def get_settings() -> Settings:
    return Settings()
