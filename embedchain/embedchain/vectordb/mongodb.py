import copy
import os
from typing import Any, Optional, Union

try:
    from pymongo import MongoClient
except ImportError:
    raise ImportError("MongoDB requires extra dependencies. Install with `pip install embedchain[mongodb]`") from None

from embedchain.config.vector_db.mongodb import MongoDBConfig
from embedchain.vectordb.base import BaseVectorDB

class MongoDB(BaseVectorDB):
    """
    MongoDB as vector database
    """

    def __init__(self, config: MongoDBConfig = None):
        """
        MongoDB as vector database
        :param config. MongoDB database config to be used for connection
        """
        if config is None:
            config = MongoDBConfig()
        else:
            if not isinstance(config, MongoDBConfig):
                raise TypeError(
                    "config is not a `MongoDBConfig` instance. "
                    "Please make sure the type is right and that you are passing an instance."
                )
        self.config = config
        self.client = MongoClient(os.getenv("MONGODB_URI"))
        self.db = self.client[self.config.database_name]
        super().__init__(config=self.config)

    def _initialize(self):
        """
        This method is needed because `embedder` attribute needs to be set externally before it can be initialized.
        """
        if not self.embedder:
            raise ValueError("Embedder not set. Please set an embedder with `set_embedder` before initialization.")

        self.collection_name = self._get_or_create_collection()
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name)

    def _get_or_create_db(self):
        return self.db

    def _get_or_create_collection(self):
        return self.config.collection_name

    def get(self, ids: Optional[list[str]] = None, where: Optional[dict[str, Any]] = None, limit: Optional[int] = None):
        """
        Get existing doc ids present in vector database
        :param ids: _list of doc ids to check for existence
        :type ids: list[str]
        :param where: to filter data
        :type where: dict[str, any]
        :param limit: The number of entries to be fetched
        :type limit: Optional int, defaults to None
        :return: All the existing IDs and their metadata
        :rtype: dict
        """
        query = {}
        if ids:
            query["identifier"] = {"$in": ids}
        if where:
            query.update({"metadata." + k: v for k, v in where.items()})

        cursor = self.db[self.collection_name].find(query, limit=limit)
        existing_ids = []
        metadatas = []
        for doc in cursor:
            existing_ids.append(doc["identifier"])
            metadatas.append(doc["metadata"])
        return {"ids": existing_ids, "metadatas": metadatas}

    def add(
        self,
        documents: list[str],
        metadatas: list[object],
        ids: list[str],
        **kwargs: Optional[dict[str, any]],
    ):
        """Add data in vector database
        :param documents: list of texts to add
        :type documents: list[str]
        :param metadatas: list of metadata associated with docs
        :type metadatas: list[object]
        :param ids: ids of docs
        :type ids: list[str]
        """
        embeddings = self.embedder.embedding_fn(documents)

        for id, document, metadata, embedding in zip(ids, documents, metadatas, embeddings):
            metadata["text"] = document
            doc = {
                "identifier": id,
                "text": document,
                "metadata": copy.deepcopy(metadata),
                "embedding": embedding.tolist(),
            }
            self.db[self.collection_name].insert_one(doc)

    def query(
        self,
        input_query: str,
        n_results: int,
        where: dict[str, any],
        citations: bool = False,
        **kwargs: Optional[dict[str, Any]],
    ) -> Union[list[tuple[str, dict]], list[str]]:
        """
        Query contents from vector database based on vector similarity
        :param input_query: query string
        :type input_query: str
        :param n_results: number of similar documents to fetch from database
        :type n_results: int
        :param where: Optional. filter data
        :type where: dict[str, any]
        :param citations: boolean to return context along with the answer
        :type citations: bool, default is False
        :return: The content of the document that matched your query, 
                 along with the source and doc_id (if citations flag is true)
        :rtype: list[str], if citations=False, otherwise list[tuple[str, dict]]
        """
        query_vector = self.embedder.embedding_fn([input_query])[0]
        query = {"embedding": {"$near": query_vector.tolist()}}
        if where:
            query.update({"metadata." + k: v for k, v in where.items()})

        cursor = self.db[self.collection_name].find(query).limit(n_results)
        contexts = []
        for doc in cursor:
            context = doc["text"]
            if citations:
                metadata = doc["metadata"]
                metadata["score"] = doc.get("score", 0) 
                contexts.append((context, metadata))
            else:
                contexts.append(context)
        return contexts

    def count(self) -> int:
        return self.db[self.collection_name].count_documents({})

    def reset(self):
        self.db.drop_collection(self.collection_name)
        self._initialize()

    def set_collection_name(self, name: str):
        """
        Set the name of the collection. A collection is an isolated space for vectors.
        :param name: Name of the collection.
        :type name: str
        """
        if not isinstance(name, str):
            raise TypeError("Collection name must be a string")
        self.config.collection_name = name
        self.collection_name = self._get_or_create_collection()

    @staticmethod
    def _generate_query(where: dict):
        query = {}
        for key,value in where.items():
            query[f"metadata.{key}"] = value
        
        return query
    
    def delete(self, where: dict):
        """
        Delete documents from the vector database
        :param where: The conditions to filter documents for deletion
        :type where: dict
        """
        query = {"metadata." + k: v for k, v in where.items()}
        self.db[self.collection_name].delete_many(query)
    