from pydantic import BaseModel

class ClickHouseConfig(BaseModel):
    host: str
    port: int
    username: str  # Ensure this is present
    password: str
    database: str
    jwt_token: str = None  # Add jwt_token if necessary, and make it optional (default to None)

