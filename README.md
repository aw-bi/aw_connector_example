# Пример пользовательского коннектора для AW BI

Это проект с примером пользовательского (кастомного) коннектора к AW BI на языке Python с использованием [FastAPI](https://fastapi.tiangolo.com/).

| Путь в репозитории | Описание |
| --- | --- |
| src/aw_connector_example/routers | Реализации всех конечных точек API для подключения к AW BI |
| src/aw_connector_example/dto.py | Описание DTO-объектов для обмена данными по API в виде [Pydantic-моделей](https://docs.pydantic.dev/) |
| src/aw_connector_example/data | Данные пользовательского источника в виде json-файлов |

## Установка коннектора

### Подготовка сервера

Установите [Docker](https://docs.docker.com/engine/install/). Для Windows можно воспользоваться:
* [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/)
* установкой Docker в WSL [Установка WSL (Windows Subsystem for Linux) | Microsoft Learn](https://learn.microsoft.com/ru-ru/windows/wsl/install) + [Install Docker Engine on Ubuntu | Docker Documentation](https://docs.docker.com/engine/install/ubuntu/).

### Подготовка AW BI

Убедитесь, что доступ к сервису S3 (контейнер minio в AW BI) открыт для запросов извне docker-сети. Для этого, зайдите по ssh на сервер, где
запущен сервис минио и выведите его состояние:

```sh
$ cd /opt/aw/app

$ docker compose ps minio

SERVICE   CREATED              STATUS                        PORTS
minio     About a minute ago   Up About a minute (healthy)   0.0.0.0:8181->9000/tcp, 0.0.0.0:8182->9001/tcp, [::]:8182->9001/tcp
```

Здесь видно, что доступ к S3 API открыт на порту 8181. Если такой информации в выводе `docker compose ps` нет, то установите
значение переменной `MINIO_S3_PORT` в `.env` файле AW BI (обычно, он располагается по пути `/opt/aw/app/.env`) и пересоздайте контейнер `docker compose up -d minio`.

### Настройка переменных окружения

Скопируйте файл `.env.dist` в `.env`. Заполните значения обязательных переменных `CONNECTOR_PORT` и `ETL_S3_URL`.


| Переменная | Обязательно | Описание |
| --- | --- | --- |
| <nobr>`CONNECTOR_PORT`</nobr>| да | Порт (или IP адрес + порт) на котором необходимо поднять сервис коннектора.<br><br>Если указывается просто порт, то коннектор запускается на интерфейсе 0.0.0.0. Чтобы ограничить доступ к коннектору только из локальной сети или с локальной машины укажите IP адрес соответствующего сетевого интерфейса.<br><br>Примеры значений:<ul><li>`9911`</li><li>`192.168.1.9:9911`</li><li>`127.0.0.1:9911`</li><ul> |
| <nobr>`ETL_S3_URL`</nobr> | да | URL к S3 хранилищу AW BI (порт для доступа к нему открывался выше в разделе "Подготовка AW BI"). Аутентификационные данные указываются в самом URL. Пример значения: <nobr>http://user:password@192.168.1.8:8181</nobr>|
| <nobr>`ETL_S3_BUCKET`</nobr> | нет<br>Значение по умолчанию: <nobr>`aw-etl`</nobr> | Название бакета в S3, который используется в подсистеме ETL AW BI  |
| <nobr>`LOG_LEVEL`</nobr>| нет<br>Значение по умолчани.: `info` | Уровень логирования. При установке значения `debug` в консоли сервиса видны тела запросов и ответов. Указывается одно из значений: `trace`, `debug`, `info`, `warning`, `error`, `critical`.


### Запуск коннектора

```sh
$ docker compose build && docker compose up -d
```

После запуска убедитесь, что контейнер перешел в состояние `healthy`.

```sh
$ docker compose ps

SERVICE                CREATED              STATUS                        PORTS
aw-connector-example   About a minute ago   Up About a minute (healthy)   192.168.1.9:9911->8080/tcp
```

Если контейнер перешел в состояние unhealthy, то в логах можно уточнить причину:

```sh
$ docker compose logs

botocore.exceptions.ConnectTimeoutError: Connect timeout on endpoint URL: "http://192.168.1.135:8181/aw-etl?list-type=2&prefix=&delimiter=%2F&encoding-type=url"
```

В большинстве случаев, это связано с некорректными настройками переменных `ETL_S3_URL` и `ETL_S3_BUCKET`.

## API документация

Для просмотра документации пройдите в браузере по адресу http://127.0.0.1:9911/docs (здесь, вместо порта 9911 может понадобиться указать значение из перемнной окружения`CONNECTOR_PORT`).

## Подключение коннектора к AW BI

Возможность подключения пользовательских коннекторов реализована в AW BI начиная с версии 1.36.

### Добавление типа источника

Зайдите по ssh на сервер, внутри которого запущен контейнер `db` из поставки AW BI, и запустите
консоль этого контейнера:

```shell
$ cd /opt/aw/app

$ docker compose exec db bash
```

Внутри контейнера подключитесь к базе данных AW BI. Здесь analytic - это `DB_USERNAME`, и workspace - это `DB_NAME` из .env файла AW BI (обычно, он располагается по пути `/opt/aw/app/.env`)

```shell
$ psql -U analytic workspace
```

Проверьте текущие зарегистрированные типы источников:

```sql
workspace=# SELECT * FROM public.data_source_type;

 id |        title         | driver_name  | default_port | is_file |       endpoint_url        
----+----------------------+--------------+--------------+---------+---------------------------
  1 | PostgreSQL           | pgsql        |         5432 | f       | 
  2 | MySQL                | mysql        |         3306 | f       | 
  3 | Oracle               | oracle       |         1521 | f       | 
  6 | Greenplum            | greenplum    |         5432 | f       | 
 10 | Vertica              | vertica      |         5433 | f       | 
 11 | MariaDB              | mariadb      |         3306 | f       | 
 12 | Microsoft SQL Server | mssql        |         1433 | f       | 
 13 | Файл                 | file         |              | t       | 
 14 | Firebird             | firebird     |         3050 | f       | 
 15 | Red                  | red          |         3050 | f       | 
  4 | ClickHouse           | clickhouse   |         9000 | f       | 
 16 | OData                | odata        |              | f       | 
 17 | 1C                   | 1c           |              | f       | 
 (13 rows)
```

Для добавления нового типа источника необходимо указать следующие параметры:

| Название | Тип | Описание |
| --- | --- | --- |
| <nobr>`title`</nobr> | string | Название типа источника. Показывается в пользовательском интерфейсе при настройке источников данных. |
| <nobr>`driver_name`</nobr> | string | Уникальная строка, которая идентифицирует коннектор. Придумайте свое название и укажите здесь. |
| <nobr>`endpoint_url`</nobr> | string | Базовый URL, который используется для HTTP запросов к коннектору. Это значение соответствует значению переменной `CONNECTOR_URL` из настроек коннектора. Указанные в этом URL хост и порт должны быть доступны из всех контейнеров AW BI. Например, `http://192.168.1.9:9911` |

Пример SQL запроса на вставку строки:
```sql
INSERT INTO public.data_source_type
(
    id, 
    title, 
    driver_name, 
    endpoint_url
)
VALUES
(
    nextval('data_source_type_id_seq'::regclass), 
    'Мой источник', 
    'my-connector', 
    'http://192.168.1.9:9911'
);
```

### Проверка добавленного типа источника

Зайдите в список источников AW BI и создайте новый источник. В списке доступных типов 
должна появиться ваша запись.

![](docs/images/select-new-data-source-type.png)

Если форма добавления нового источника перестала открываться, то зайдите в консоль контейнера `db` 
и удалите только что добавленную запись:

```sql
workspace=# DELETE FROM public.data_source_type WHERE id=...;
```

### Проверка подключения к коннектору

Заполните параметры подключения на форме создания источника данных и сохраните запись:

* Имя подключения: любое значение;
* Имя хоста: любое значение;
* Имя базы данных: `db1`;
* Порт: любое значение;
* Логин: любое значение;
* Пароль: любое значение;
* Тип соединения: `Обычное`

Убедитесь, что все операции с источником (просмотр таблиц и данных источнкиа, добавление в модель, предпросмотр модели, загрузка данных модели)
выполняются корректно.