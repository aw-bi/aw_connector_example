from typing import Any
from enum import Enum

from pydantic import BaseModel, Field


class DataSource(BaseModel):
    """
    Описание источника данных
    """

    id: int = Field(
        description='Идентификатор источника в AW BI', examples=[10]
    )

    type: str = Field(description='Тип источника', examples=['my-data-source'])

    params: dict[str, Any] = Field(
        description='Параметры подключения к источнику',
        examples=[
            {
                'host': '192.168.1.1',
                'port': 7777,
                'db': 'db1',
                'username': 'test',
                'password': 'test',
            }
        ],
    )

    extra: dict[str, Any] | None = Field(
        description='Дополнительные параметры источника',
        default=None,
        examples=[
            {},
            {'Таймаут': '600', 'ПостраничныйВывод': 'Да'},
        ],
    )


class DataSourceObject(BaseModel):
    """
    Метаданные объекта в источнике
    """

    schema_name: str = Field(
        alias='schema',
        description='Название схемы (каталога), в которой находится объект источника',
        examples=['public'],
    )

    name: str = Field(title='Название объекта', examples=['my_table'])

    type: str = Field(title='Тип объекта', examples=['table'])


class SimpleType(str, Enum):
    """
    Типы полей, которые используются в AW
    """

    string = 'string'
    number = 'number'
    float = 'float'
    date = 'date'
    bool = 'bool'


class ObjectColumnMeta(BaseModel):
    """
    Метаданные столбца объекта
    """

    name: str = Field(description='Название столбца', examples=['column_name'])
    type: str = Field(description='Исходный тип столбца', examples=['VARCHAR(10)'])
    simple_type: SimpleType = Field(description='Тип поля для AW', examples=['string'])
    comment: str | None = Field(
        description='Комментарий к полю источника', default=None
    )


class ForeignKeyMeta(BaseModel):
    """
    Описание связи к внешнему объекту источника
    """

    column_name: str = Field(description='Столбец в текущем объекта', examples=['id'])
    foreign_table_schema: str = Field(description='Схема внешнего объекта', examples=['public'])
    foreign_table_name: str = Field(description='Название внешнего объекта', examples=['table2'])
    foreign_column_name: str = Field(description='Столбц внешнего объекта, связанного с column_name текеущего объекта', examples=['id'])


class ObjectMeta(BaseModel):
    """
    Метаданные объекта источника
    """

    columns: list[ObjectColumnMeta] = Field(
        description='Список столбцов модели',
        default=[],
        examples=[
            [
                {
                    'name': 'id',
                    'type': 'DECIMAL',
                    'simple_type': 'number',
                    'comment': None,
                },
                {
                    'name': 'name',
                    'type': 'VARCHAR(10)',
                    'simple_type': 'string',
                    'comment': 'Наименование',
                },
            ]
        ],
    )
    foreign_keys: list[ForeignKeyMeta] = Field(
        title='Список связей объекта источника',
        description='Перечисление внешних ключей объекта источника',
        default=[],
    )


class ObjectData(BaseModel):
    """
    Данные объекта источника
    """

    data: list[dict] = Field(
        description='Данные объекта источника',
        examples=[
            [
                {
                    'id': 1,
                    'name': 'name 1',
                },
                {
                    'id': 2,
                    'name': 'name 2',
                },
                {
                    'id': 3,
                    'name': 'name 3',
                },
            ]
        ],
    )


class FilterDto(BaseModel):
    """ 
    Условие на выгрузку данных в parquet
    """
    field_name: str | None = None
    operator: str | None = None
    value: Any


class ParquetObjectFieldDto(BaseModel):
    """ 
    Поле в объекта
    """
    name: str = Field(description='Название поля AW')
    type: SimpleType = Field(description='Тип поля AW BI')


class ParquetObjectDto(BaseModel):
    """
    Объект, для которого выполняется выгрузка данных в parquet
    """

    name: str = Field(
        ...,
        title='Название объекта',
        description='Название объекта указывается в формате schema.table (например, public.table1)',
        examples=['public.table2'],
    )

    type: str = Field(..., description="Тип объекта источника (sql или тип объекта источника)", examples=['table'])
    query_text: str | None = Field(default=None, description='Текст SQL запроса для объекта источника. Используется в случае type: sql', examples=[None])

    data_source: DataSource = Field(..., description='Описание источника данных')



# -----------------------------------------------------------------------
# DTO для запросов и ответов
# -----------------------------------------------------------------------
class PingRequest(BaseModel):
    """
    Тело запроса на проверку работоспособности источника. Совпадает с моделью DataSource
    за исключением того, что id может быть None
    """

    id: int | None = Field(
        description='Идентификатор источника в AW BI (или None, если источник данных еще не создан)',
        default=None,
        examples=[10],
    )

    type: str = Field(description='Тип источника', examples=['my-data-source'])

    params: dict[str, Any] = Field(
        title='Параметры подключения к источнику',
        description='Здесь указываются все параметры, которые были введены на форме создания источнкиа',
        examples=[
            {
                'host': '192.168.1.1',
                'port': 7777,
                'db': 'db1',
                'username': 'test',
                'password': 'test',
            }
        ],
    )

    extra: dict[str, Any] | None = Field(
        default=None,
        title='Дополнительные параметры источника',
        examples=[
            {},
            {'Таймаут': '600', 'ПостраничныйВывод': 'Да'},
        ],
    )


class ObjectListRequest(BaseModel):
    """
    Модель запроса за списком объектов источника
    """

    data_source: DataSource = Field(..., description='Описание источника данных')

    query_string: str | None = Field(
        default=None, description='Фильтр на название объекта', examples=[None]
    )

    flat: bool | None = Field(
        description='Вернуть данные в плоском или иерархическом виде (по умолчанию, в плоском виде)',
        default=True,
        examples=[True],
    )


class ObjectMetaRequest(BaseModel):
    """
    Модель запроса метаданных объекта источника
    """

    data_source: DataSource = Field(..., description='Описание источника данных')
    object_name: str = Field(
        ...,
        description='Название объекта в формате {schema}.{name}',
        examples=['public.table3'],
    )


class ObjectDataRequest(BaseModel):
    """
    Модель запроса за данным объекта
    """

    data_source: DataSource = Field(..., description='Описание источника данных')
    object_name: str = Field(
        ..., description='Название объекта', examples=['public.table3']
    )
    page: int | None = Field(
        default=1,
        description='Индекс страницы с данными (нумерация начинается с 0)',
        examples=[1],
    )
    page_size: int | None = Field(
        default=20, description='Количество строк на странице', examples=[20]
    )


class SqlMetaRequest(BaseModel):
    """
    Запроа метаданных источника по SQL запросу
    """

    data_source: DataSource = Field(
        ...,
        title='Источник данных',
        description='Источник данных, в котором будет выполнен SQL запрос',
    )
    sql_text: str = Field(
        ...,
        title='Текст SQl запроса',
        description='Текст SQL запроса указывается в диалекте источника',
        examples=['select * from table1'],
    )


class SqlDataRequest(BaseModel):
    """
    Запрос данных источника по SQL запросу
    """

    data_source: DataSource = Field(
        ...,
        title='Источник данных',
        description='Источник данных, в котором будет выполнен SQL запрос',
    )
    sql_text: str = Field(
        ...,
        title='Текст SQl запроса',
        description='Текст SQL запроса указывается в диалекте источника',
        examples=['select * from table1'],
    )
    page: int | None = Field(
        default=1,
        description='Индекс страницы с данными (нумерация начинается с 0)',
        examples=[1],
    )
    page_size: int | None = Field(
        default=20, description='Количество строк на странице', examples=[20]
    )


class ParquetRequest(BaseModel):
    """
    Запрос на выгрузку данных в parquet
    """

    object: ParquetObjectDto = Field(
        ...,
        description='Объект в источнике данных, для которого нужно выгрузить данные',
    )
    folder: str = Field(
        ...,
        description='Путь к S3-папке, в которую нужно выгрузить данные',
        examples=['s3://runs/2025-08-21_01-02-03-preview-68d9/data.parquet'],
    )
    filters: list[FilterDto] | None = Field(
        default=None,
        description='Список условий, которые нужно применить к выборке записей. Условия соединяются через AND',
        examples=[None]
    )
    limit: int | None = Field(
        default=None,
        description='Ограничение на количество записей, которое нужно выгрузить в parquet',
        examples=[None]
    )
