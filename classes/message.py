from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum


class Sentiment(IntEnum):
    BULLISH = 1
    BEARISH = 0
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


def to_dict(message: Message) -> dict:
    return {
        "body": message.body,
        "id": message.id,
        "created_at": message.created_at,
        "symbols": message.symbols,
        "sentiment": int(message.sentiment),
        "likes": message.likes,
        "replies": message.replies
    }
