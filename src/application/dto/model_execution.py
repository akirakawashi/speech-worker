"""Контракт 1.0: исходные данные и подготовленный бэкендом запрос к модели."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

ModelScenario = Literal["analysis", "reason", "free_classification", "operator_compare"]
ModelProfile = Literal["production", "candidate"]


class ModelMessage(BaseModel):
    """Роль и текст сообщения; содержимое разговора не выводится в repr."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1, repr=False)


class ModelPreparationRequest(BaseModel):
    """Выбрать сценарий и передать его именованные входы без HTTP-объектов."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    scenario: ModelScenario
    profile: ModelProfile = "production"
    inputs: dict[str, JsonValue] = Field(repr=False)


class PreparedModelRequest(BaseModel):
    """Готовый запрос; адрес модели и секреты остаются у исполнителя."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"]
    scenario: ModelScenario
    profile: ModelProfile
    prompt_version: str = Field(min_length=1)
    model: str = Field(min_length=1)
    messages: tuple[ModelMessage, ...] = Field(min_length=1, repr=False)
    temperature: float = Field(ge=0, le=2)
    max_tokens: int = Field(ge=1, le=32768)
