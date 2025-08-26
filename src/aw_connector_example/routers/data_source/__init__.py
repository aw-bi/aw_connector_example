from fastapi import APIRouter

router = APIRouter(prefix='/data-source')

from .ping import *
from .objects import *
from .object_meta import *
from .object_data import *
from .sql_meta import *
from .sql_data import *
from .parquet import *

