from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    azure_openai_api_key: str 
    azure_openai_endpoint: str
    deployment_model: str
    embedding_model: str