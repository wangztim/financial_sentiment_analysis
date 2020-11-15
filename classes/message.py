from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Sentiment(Enum):
    NEUTRAL = 0
    BULLISH = 1
    BEARISH = -1
    UNDEFINED = -69


@dataclass(frozen=True)
class Message:
    body: str
    id: str
    created_at: datetime
    sentiment: Sentiment = Sentiment.UNDEFINED
    symbols: [str]
    likes: int = 0
