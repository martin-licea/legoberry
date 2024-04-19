from pydantic import BaseModel

class ConfigModel(BaseModel):
    name: str
    age: int
    address: str