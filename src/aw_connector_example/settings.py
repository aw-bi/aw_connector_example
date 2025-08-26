from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """ 
    """
    etl_s3_url: str = ''
    etl_s3_bucket: str = ''
