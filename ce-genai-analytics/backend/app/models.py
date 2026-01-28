from typing import List, Dict, Optional
from pydantic import BaseModel, Field


# =========================
# Chat API Contracts
# =========================

class ChatRequest(BaseModel):
    message: str = Field(..., description="User natural language business question")


class ChatResponse(BaseModel):
    intent: dict
    sql: str
    data: List[Dict]
    narrative: str


# =========================
# Structured Query Intent
# (What GPT should return)
# =========================

class TimeRange(BaseModel):
    period: Optional[str] = Field(None, description="Q1, Q2, Q3, Q4, MTD, YTD")
    start: Optional[str] = Field(None, description="YYYY-MM-DD")
    end: Optional[str] = Field(None, description="YYYY-MM-DD")


class Comparison(BaseModel):
    enabled: bool = False
    previous_period: Optional[str] = None


class Ranking(BaseModel):
    order_by: Optional[str] = None
    limit: Optional[int] = None


class QueryIntent(BaseModel):
    metrics: List[str] = Field(default_factory=list)
    dimensions: List[str] = Field(default_factory=list)
    filters: Dict[str, str] = Field(default_factory=dict)
    time_range: Optional[TimeRange] = None
    comparison: Optional[Comparison] = None
    ranking: Optional[Ranking] = None
    narrative: bool = True


# =========================
# Internal Execution Models
# =========================

class SqlExecutionResult(BaseModel):
    sql: str
    params: Dict[str, str]
    rows: List[Dict]


class NarrativeResult(BaseModel):
    narrative: str
    insights: Optional[List[str]] = None
