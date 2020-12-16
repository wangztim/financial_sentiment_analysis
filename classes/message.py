from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum


class Sentiment(IntEnum):
    NEUTRAL = 0
    BULLISH = 1
    BEARISH = -1
    UNDEFINED = -69


@dataclass()
class Message:
    body: str
    id: str
    created_at: datetime
    symbols: [str]
    sentiment: Sentiment = Sentiment.UNDEFINED
    likes: int = 0
    replies: int = 0
