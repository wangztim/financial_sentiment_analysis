from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum


class Sentiment(IntEnum):
    BULLISH = 1
    BEARISH = 0
    UNCERTAIN = -69


@dataclass()
class Message:
    id: str
    body: str
    author: str
    created_at: datetime
    source: str
    sentiment: Sentiment = None
    likes: int = 0
    replies: int = 0


def to_dict(message: Message) -> dict:
    return {
        "body": message.body,
        "id": message.id,
        "created_at": message.created_at,
        "sentiment":
        int(message.sentiment) if message.sentiment is not None else None,
        "likes": message.likes,
        "replies": message.replies,
        "source": message.source,
        "author": message.author
    }


def to_tuple(message: Message):
    return (message.id, message.body, message.author, message.created_at,
            int(message.sentiment) if message.sentiment is not None else None,
            message.source, message.likes, message.replies)
