from typing import Any
from pathlib import Path

import os
import json
import datetime

import aiofiles.os
from sqlglot import parse_one, exp
import polars
from polars.sql import SQLContext

from aw_connector_example.dto import (
    DataSource,
    DataSourceObject,
    ObjectMeta,
    ObjectColumnMeta,
    SimpleType,
    ParquetFilterExpr,
)


class DataRepositoryError(Exception):
    """
    Исключение, выбрасываемое в коде DataRepository
    """


class DataRepository:
    """
    Репозиторий доступа к данным
    """

    def __init__(self, root_folder: Path):
        self.root = root_folder

    async def ping_data_source(self, data_source: DataSource):
        """
        Проверяет возможность подключения к источнику данных.
        Если подключение не удалось, то выбрасывается исключение DataRepositoryError

        Args
        ---------------------
        data_source : DataSource
            Описание источника данных, к которому проверяется подключение
        """
        db_name, db_path = self.get_db(data_source)
        if not db_path.exists():
            raise DataRepositoryError(f'База данных {db_name} не найдена')

    async def get_objects(
        self, data_source: DataSource, query_string: str | None = None
    ) -> list[DataSourceObject]:
        """
        Возвращает список объектов из источника данных

        Args
        -----------------------
        data_source : DataSource
            Описание источника данных
        query_string : str | None
            Возвратить только те объекты источника, в названии которых есть
            эта подстрока
        """
        objects = []

        if 'db' not in data_source.params:
            raise DataRepositoryError('Не указано название базы данных')

        db_name = str(data_source.params['db'])
        db_path = self.root / db_name
        if not db_path.exists():
            raise DataRepositoryError(f'База данных {db_name} не найдена')

        for schema in await aiofiles.os.listdir(db_path):
            for file in await aiofiles.os.listdir(db_path / schema):
                table_name = os.path.splitext(file)[0]
                if not query_string or query_string in table_name:
                    objects.append(
                        DataSourceObject(
                            schema=schema,
                            name=table_name,
                            type='table',
                        )
                    )

        return objects

    async def get_object_meta(
        self, data_source: DataSource, object_name: str
    ) -> ObjectMeta:
        """
        Возвращает метаданные источника данных
        """
        rows = await self.get_rows(data_source, object_name)

        return ObjectMeta(
            columns=self.get_columns_meta_for_row(rows[0]), foreign_keys=[]
        )

    async def get_object_data(
        self,
        data_source: DataSource,
        object_name: str,
        limit: int | None = None,
        offset: int | None = None,
        filters: list[ParquetFilterExpr] | None = None,
    ) -> list[dict]:
        """ """
        rows = await self.get_rows(data_source, object_name)

        if filters:
            rows = self.apply_filters(rows, filters)

        return (
            rows[offset : offset + limit]
            if limit is not None and offset is not None
            else rows
        )

    async def get_sql_meta(self, data_source: DataSource, sql_text: str) -> ObjectMeta:
        """
        Получение метаданных SQL запроса
        """
        rows = await self.get_sql_rows(data_source, sql_text)

        return ObjectMeta(
            columns=self.get_columns_meta_for_row(rows[0]), foreign_keys=[]
        )

    async def get_sql_data(
        self,
        data_source: DataSource,
        sql_text: str,
        limit: int | None = None,
        offset: int | None = None,
        filters: list[ParquetFilterExpr] | None = None,
    ) -> list[dict]:
        """
        Получение данных SQL запроса
        """
        rows = await self.get_sql_rows(data_source, sql_text)

        if filters:
            rows = self.apply_filters(rows, filters)

        return (
            rows[offset : offset + limit]
            if limit is not None and offset is not None
            else rows
        )

    # --------------------------------------------------------------------
    # Внутренние методы
    # --------------------------------------------------------------------
    def get_db(self, data_source: DataSource) -> tuple[str, Path]:
        """
        Возвращает кортеж с названием базы данных и пути, где хранятся ее объекты
        """
        if 'db' not in data_source.params:
            raise DataRepositoryError(
                'В описании источника не указано название базы данных'
            )
        db_name = str(data_source.params['db'])
        db_path = self.root / db_name

        return db_name, db_path

    async def get_rows(self, data_source: DataSource, object_name: str) -> list[dict]:
        """
        Возвращает строки из базы данных
        """
        db_name, db_path = self.get_db(data_source)
        if not db_path.exists():
            raise DataRepositoryError(f'База данных {db_name} не найдена')

        if '.' not in object_name:
            raise DataRepositoryError(
                'В названии объекта должна быть указана схема (например, public.table_name)'
            )

        schema, table = object_name.split('.', maxsplit=1)
        table_path = db_path / schema / f'{table}.json'
        if not table_path.exists():
            raise DataRepositoryError(
                f'Таблица {table} не найдена в базе данных {db_name}'
            )

        async with aiofiles.open(table_path, mode='r') as f:
            return json.loads(await f.read())

    async def get_sql_rows(self, data_source: DataSource, sql_text: str) -> list[dict]:
        """ """
        dfs = {}

        for table in parse_one(sql_text).find_all(exp.Table):
            data_source_object = next(
                (
                    o
                    for o in await self.get_objects(data_source)
                    if o.name == table.name
                ),
                None,
            )
            if data_source_object is None:
                raise DataRepositoryError(
                    f'Таблица {table.name} из SQL запроса не найдена в источнике'
                )
            object_name = f'{data_source_object.schema_name}.{data_source_object.name}'
            dfs[table.name] = polars.from_dicts(
                await self.get_rows(data_source, object_name)
            )

        ctx = SQLContext()
        for table_name, table_df in dfs.items():
            ctx.register(table_name, table_df)

        return ctx.execute(sql_text).collect().to_dicts()
    
    @staticmethod
    def apply_filters(rows: list[dict], filters: list[ParquetFilterExpr]) -> list[dict]:
        """ 
        Применяет список фильтров к строкам данных
        """
        ctx = SQLContext()
        ctx.register('tbl', polars.from_dicts(rows))

        where = ' and '.join(
            [
                (
                    f'{f.field_name} {f.operator} {f.value}'
                    if f.field_name
                    else f.value
                )
                for f in filters
            ]
        )
        return ctx.execute(f'select * from tbl where {where}').collect().to_dicts()

    @staticmethod
    def get_columns_meta_for_row(row: dict) -> list[ObjectColumnMeta]:
        """
        Возвращает метаданные по строке данных
        """
        columns: list[ObjectColumnMeta] = []
        for key, value in row.items():
            columns.append(
                ObjectColumnMeta(
                    name=key,
                    type=type(value).__name__,
                    simple_type=DataRepository.get_simple_type_by_value(value),
                    comment=None,
                )
            )

        return columns

    @staticmethod
    def get_simple_type_by_value(value: Any) -> SimpleType:
        """ """
        match value:
            case int():
                return SimpleType.number
            case float():
                return SimpleType.float
            case bool():
                return SimpleType.bool
            case datetime.date() | datetime.datetime():
                return SimpleType.date
            case _:
                return SimpleType.string
