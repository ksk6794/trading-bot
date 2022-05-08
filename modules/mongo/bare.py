import copy
import decimal
from typing import List, Dict, Optional, Tuple, Type

import pymongo
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from pymongo.client_session import ClientSession


class BareMongoClient:  # pragma: no cover
    def __init__(self, mongo_uri: str, indexes: Dict):
        self._client = AsyncIOMotorClient(mongo_uri)
        self._indexes = indexes
        self._db = self._client.get_database()

    def __getattr__(self, item):
        return getattr(self._client, item)

    async def connect(self):
        await self._client.server_info()

        if self._indexes:
            for model, indexes in self._indexes.items():
                collection = self._get_collection(model)
                indexes and collection.create_indexes(indexes)

    async def disconnect(self):
        await self._client.close()

    async def get(self, model: Type[BaseModel], query: Dict) -> Optional[BaseModel]:
        collection = self._get_collection(model)
        data = await collection.find_one(query)

        if data:
            return self._to_model(model, data)

        return None

    async def find_iter(
            self,
            model: Type[BaseModel],
            query: Dict,
            sort: List[Tuple[str, int]] = None,
            skip: Optional[int] = None,
            limit: Optional[int] = None
    ):
        collection = self._get_collection(model)
        cursor = collection.find(query)

        if sort is not None:
            cursor = cursor.sort(sort)

        if skip is not None:
            cursor = cursor.skip(skip)

        if limit is not None:
            cursor = cursor.limit(limit)

        async for record in cursor:
            item = self._to_model(model, record)
            yield item

    async def find(
            self,
            model: Type[BaseModel],
            query: Dict,
            sort: List[Tuple[str, int]] = None,
            skip: Optional[int] = None,
            limit: Optional[int] = None
    ) -> List[BaseModel]:
        result = []
        async for record in self.find_iter(model, query, sort, skip, limit):
            result.append(record)
        return result

    async def count(
            self,
            model: Type[BaseModel],
            query: Optional[Dict] = None
    ) -> int:
        collection = self._get_collection(model)

        if not query:
            count = await collection.estimated_document_count()
        else:
            count = await collection.count_documents(query)

        return count

    async def create(
            self,
            model: BaseModel,
            session: Optional[ClientSession] = None
    ) -> str:
        collection = self._get_collection(model)
        data = self._convert(self._to_document(model))
        result = await collection.insert_one(data, session=session)
        return str(result.inserted_id)

    async def update(
            self,
            model: BaseModel,
            query: Dict,
            session: Optional[ClientSession] = None
    ):
        collection = self._get_collection(model)
        data = self._convert(self._to_document(model))
        return await collection.replace_one(query, data, session=session)

    async def partial_update(
            self,
            model: Type[BaseModel],
            update_fields: Dict,
            query: Dict,
            session: Optional[ClientSession] = None
    ):
        collection = self._get_collection(model)
        fields = {'$set': self._convert(update_fields)}
        res = await collection.find_one_and_update(
            filter=query,
            update=fields,
            return_document=pymongo.ReturnDocument.AFTER,
            session=session,
        )

        if res:
            return self._to_model(model, res)

    async def upsert(
            self,
            model: BaseModel,
            query: Dict,
            session: Optional[ClientSession] = None
    ):
        collection = self._get_collection(model)
        data = self._convert(self._to_document(model))
        await collection.replace_one(query, data, upsert=True, session=session)

    async def delete(
            self,
            model: Type[BaseModel],
            query: Dict,
            session: Optional[ClientSession] = None
    ):
        collection = self._get_collection(model)
        await collection.delete_one(query, session=session)

    async def bulk_write(self, model: Type[BaseModel], operations: List, ordered: bool = True):
        collection = self._get_collection(model)

        return await collection.bulk_write(
            self._update_operations(model, operations), ordered=ordered)

    @classmethod
    def _update_operations(cls, model, operations):
        result = []

        for operation in operations:
            operation = copy.copy(operation)

            for field_name in ('_doc', '_filter'):
                field = getattr(operation, field_name, None)

                if isinstance(field, model):
                    value = cls._to_document(field)
                    setattr(operation, field_name, value)

            result.append(operation)

        return result

    def _get_collection(self, model):
        model_class = self._get_model_class(model)
        collection_name = model_class.__name__
        collection = self._db[collection_name]
        return collection

    def _convert(self, obj):
        if isinstance(obj, dict):
            return {key: self._convert(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self._convert(item) for item in obj]
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return obj

    @staticmethod
    def _get_model_class(model):
        cls = None

        if isinstance(model, BaseModel):
            cls = model.__class__

        elif issubclass(model, BaseModel):
            cls = model

        return cls

    @staticmethod
    def _to_model(model, data):
        return model(**data)

    @staticmethod
    def _to_document(model):
        return model.dict(exclude_none=True, by_alias=True)
