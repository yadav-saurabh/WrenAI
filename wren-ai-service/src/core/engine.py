import logging
import re
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Optional, Tuple

import aiohttp
from pydantic import BaseModel

logger = logging.getLogger("wren-ai-service")


class EngineConfig(BaseModel):
    provider: str = "wren_ui"
    config: dict = {}


class Engine(metaclass=ABCMeta):
    @abstractmethod
    async def execute_sql(
        self,
        sql: str,
        session: aiohttp.ClientSession,
        dry_run: bool = True,
        **kwargs,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        ...


def clean_generation_result(result: str) -> str:
    def _normalize_whitespace(s: str) -> str:
        return re.sub(r"\s+", " ", s).strip()

    cleaned = (
        result.replace("```sql", "")
        .replace("```json", "")
        .replace('"""', "")
        .replace("'''", "")
        .replace("```", "")
    ).strip()

    json_match = re.search(r"\{\s*\"sql\"\s*:\s*\"(?P<sql>.*?)\"\s*\}", cleaned, re.DOTALL)
    if json_match:
        extracted = json_match.group("sql")
        extracted = extracted.replace('\\n', ' ').replace('\\"', '"')
        return _normalize_whitespace(extracted).rstrip(';')

    sql_match = re.search(
        r"(?is)\b(WITH|SELECT)\b.*",
        cleaned,
    )
    if sql_match:
        extracted = sql_match.group(0)
        return _normalize_whitespace(extracted).rstrip(';')

    return _normalize_whitespace(cleaned).rstrip(';')


def remove_limit_statement(sql: str) -> str:
    pattern = r"\s*LIMIT\s+\d+(\s*;?\s*--.*|\s*;?\s*)$"
    modified_sql = re.sub(pattern, "", sql, flags=re.IGNORECASE)

    return modified_sql
