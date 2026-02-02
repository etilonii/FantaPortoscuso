from pydantic import BaseModel, Field


class KeyCreateResponse(BaseModel):
    key: str


class LoginRequest(BaseModel):
    key: str = Field(min_length=8, max_length=32)
    device_id: str = Field(min_length=6, max_length=128)


class LoginResponse(BaseModel):
    status: str
    message: str
    is_admin: bool = False


class AdminKeyResponse(BaseModel):
    key: str


class AdminKeyItem(BaseModel):
    key: str
    used: bool
    is_admin: bool
    device_id: str | None = None
    created_at: str | None = None
    used_at: str | None = None


class ImportKeysRequest(BaseModel):
    keys: list[str] = Field(min_length=1)
    is_admin: bool = False


class ImportTeamKeyItem(BaseModel):
    key: str = Field(min_length=1)
    team: str = Field(min_length=1)


class ImportTeamKeysRequest(BaseModel):
    items: list[ImportTeamKeyItem] = Field(min_length=1)
