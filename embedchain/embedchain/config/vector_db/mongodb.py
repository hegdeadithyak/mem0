from typing import Optional

from embedchain.config.vector_db.base import BaseVectorDbConfig
from embedchain.helpers.json_serializable import register_deserializable


@register_deserializable
class MongoDbConfig(BaseVectorDbConfig):
    def __init__(
        self,
        collection_name: Optional[str] = None,
        uri: Optional[str] = None,
        database_name : Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        replica_set: Optional[str] = None,
        retry_writes: Optional[bool] = True,
        app_name: Optional[str] = None,
    ):
        """
        Initializes a configuration class instance for MongoDB.

        :param collection_name: Default name for the collection, defaults to None
        :type collection_name: Optional[str], optional
        :param uri: MongoDB connection URI, defaults to None
        :type uri: Optional[str], optional
        :param database_name: Name of the MongoDB database to use, defaults to None
        :type database_name: Optional[str], optional
        :param username: Username for MongoDB authentication, defaults to None
        :type username: Optional[str], optional
        :param password: Password for MongoDB authentication, defaults to None
        :type password: Optional[str], optional
        :param replica_set: Name of the replica set, if used, defaults to None
        :type replica_set: Optional[str], optional
        :param retry_writes: Whether to retry writes on failure, defaults to True
        :type retry_writes: Optional[bool], optional
        :param app_name: The application name, defaults to None
        :type app_name: Optional[str], optional
        """

        self.uri = uri
        self.collection_name = collection_name
        self.username = username
        self.database_name = database_name
        self.password = password
        self.replica_set = replica_set
        self.retry_writes = retry_writes
        self.app_name = app_name
        super().__init__(collection_name=collection_name)