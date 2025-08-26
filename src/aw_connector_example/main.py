from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi

from aw_connector_example.routers.data_source import router as data_source_router
from aw_connector_example.routers import router
from aw_connector_example.dependencies import get_logger

description = """
Пример реализации API пользовательского коннектора для [AW BI](https://aw-bi.ru) на языке Python с использованием 
[FastAPI](https://fastapi.tiangolo.com/).

Исходный код примера коннектора: [https://github.com/aw-bi/aw_connector_example](https://github.com/aw-bi/aw_connector_example)

Подробная документация по API: [https://dev.aw-bi.ru/latest/etl/data_sources/custom_connector/build](https://dev.aw-bi.ru/latest/etl/data_sources/custom_connector/build)
"""

tags_metadata = [
    {
        'name': 'default',
        'description': 'Обшие операции',
    },
    {'name': 'data source', 'description': 'Операции с источником данных'},
    {
        'name': 'async',
        'description': 'Для асинхронной выгрузки данных в parquet',
    },
]

app = FastAPI(
    title='Пример коннектора AW BI',
    description=description,
    openapi_tags=tags_metadata,
)

app.include_router(data_source_router)
app.include_router(router)


@app.exception_handler(RequestValidationError)
def validation_error_handler(request: Request, exc: RequestValidationError):
    """
    Обработчик ошибок валидации запросов
    """
    logger = get_logger()

    def safe_error(e) -> str:
        if isinstance(e, dict):
            e.pop('input') if 'input' in e else None
            return f'{e.get("msg")} {e.get("loc")}'
        return f'{e}'

    logger.error(
        f'Ошибка валидации данных запроса {request.method} {request.url}:\n{"\n".join(["  - " + safe_error(e) for e in exc.errors()])}'
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({'detail': '; '.join(safe_error(e) for e in exc.errors())}),
    )


def custom_openapi():
    if not app.openapi_schema:
        app.openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            terms_of_service=app.terms_of_service,
            contact=app.contact,
            license_info=app.license_info,
            routes=app.routes,
            tags=app.openapi_tags,
            servers=app.servers,
        )
        for _, method_item in (app.openapi_schema.get('paths') or {}).items():
            for _, param in method_item.items():
                responses = param.get('responses')
                
                if '400' not in responses:
                    responses['400'] = {
                        'description': 'Bad Request',
                        'content': {'application/json': {'example': {'detail': 'База данных db2 не найдена'}}},
                    }
                elif 'content' not in responses['400']:
                    del responses['400']

                if '422' in responses and 'content' not in responses['422']:
                    del responses['422']
                else:
                    responses['422'] = {
                        'description': 'Validation Error',
                        'content': {'application/json': {'example': {'detail': 'Field required (\'body\', \'type\')'}}},
                    }

                responses['500'] = {
                    'description': 'Internal Server Error',
                    'content': {'application/json': {'example': {'detail': 'Внутренняя ошибка приложения: Could not connect to the endpoint URL'}}},
                }
    return app.openapi_schema

app.openapi = custom_openapi
