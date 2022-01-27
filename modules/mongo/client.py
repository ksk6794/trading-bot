from .bare import BareMongoClient
from .proxy import make_proxy


__all__ = (
    'MongoClient'
)

MongoClient = make_proxy(BareMongoClient)
