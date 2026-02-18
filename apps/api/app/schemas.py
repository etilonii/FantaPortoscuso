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
    access_token: str | None = None
    access_expires_at: str | None = None
    refresh_token: str | None = None
    refresh_expires_at: str | None = None
    warning: str | None = None


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=16)


class RefreshResponse(BaseModel):
    access_token: str
    access_expires_at: str
    refresh_token: str | None = None
    refresh_expires_at: str | None = None


class LogoutRequest(BaseModel):
    refresh_token: str = Field(min_length=16)


class AdminKeyResponse(BaseModel):
    key: str


class AdminKeyItem(BaseModel):
    key: str
    used: bool
    is_admin: bool
    device_id: str | None = None
    device_count: int = 0
    team: str | None = None
    note: str | None = None
    blocked: bool = False
    blocked_until: str | None = None
    blocked_reason: str | None = None
    created_at: str | None = None
    used_at: str | None = None
    last_seen_at: str | None = None
    online: bool = False
    reset_used: int = 0
    reset_limit: int = 3
    reset_season: str | None = None
    reset_cooldown_blocked: bool = False


class ImportKeysRequest(BaseModel):
    keys: list[str] = Field(min_length=1)
    is_admin: bool = False


class ImportTeamKeyItem(BaseModel):
    key: str = Field(min_length=1)
    team: str = Field(min_length=1)


class ImportTeamKeysRequest(BaseModel):
    items: list[ImportTeamKeyItem] = Field(min_length=1)


class ResetKeyRequest(BaseModel):
    key: str = Field(min_length=1)
    note: str | None = None


class KeyResetUsageResponse(BaseModel):
    key: str
    season: str
    used: int
    limit: int = 3
    last_reset_at: str | None = None
    cooldown_blocked: bool = False


class SetAdminRequest(BaseModel):
    key: str = Field(min_length=1)
    is_admin: bool = True


class TeamKeyRequest(BaseModel):
    key: str = Field(min_length=1)
    team: str = Field(min_length=1)


class TeamKeyItem(BaseModel):
    key: str
    team: str


class TeamKeyDeleteRequest(BaseModel):
    key: str = Field(min_length=1)


class KeyDeleteRequest(BaseModel):
    key: str = Field(min_length=1)


class KeyNoteRequest(BaseModel):
    key: str = Field(min_length=1)
    note: str | None = Field(default=None, max_length=255)


class KeyBlockRequest(BaseModel):
    key: str = Field(min_length=1)
    reason: str | None = Field(default=None, max_length=128)


class KeyUnblockRequest(BaseModel):
    key: str = Field(min_length=1)


class PingRequest(BaseModel):
    key: str = Field(min_length=8, max_length=32)
    device_id: str = Field(min_length=6, max_length=128)
