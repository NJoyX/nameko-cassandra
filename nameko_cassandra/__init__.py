from __future__ import unicode_literals, print_function, division

import inspect
import os
import weakref
from collections import namedtuple

from cassandra.cqlengine import management
from cassandra.cqlengine import models as cassandra_models
from cassandra.cqlengine.connection import register_connection, get_session, get_cluster, unregister_connection
from cassandra.cqlengine.management import sync_table
from cassandra.io.eventletreactor import EventletConnection
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.query import ConsistencyLevel
from nameko.extensions import DependencyProvider
from six import text_type, string_types
from werkzeug.utils import import_string

__author__ = 'Fill Q'
__all__ = ['Cassandra']

DEFAULT_CONNECTION_NAME = 'default'
CASSANDRA_HOSTS = 'CASSANDRA_HOSTS'
SIMPLE_KEYSPACE = 'simple'
NETWORK_KEYSPACE = 'network'
KEYSPACES_FUNCTIONS = {
    SIMPLE_KEYSPACE: management.create_keyspace_simple,
    NETWORK_KEYSPACE: management.create_keyspace_network_topology
}


class CassandraHelper(namedtuple('CassandraHelper', 'keyspace connection cluster session models')):
    keyspace = None
    connection = None
    cluster = None
    session = None
    _prepared_stmts = weakref.WeakValueDictionary()
    _models = dict()

    def __init__(self, *args, **kwargs):
        super(CassandraHelper, self).__init__(*args, **kwargs)
        for model in self.models:
            self._models[model.__name__] = model

    def register_user_type(self, name, obj):
        return self.cluster.register_user_type(self.keyspace, name, obj)

    def prepared_statement(self, name, query, consistency_level=ConsistencyLevel.QUORUM):
        if name in self._prepared_stmts:
            return self._prepared_stmts[name]
        self._prepared_stmts[name] = self.session.prepare(query)
        if consistency_level in ConsistencyLevel.value_to_name:
            self._prepared_stmts[name].consistency_level = consistency_level
        return self._prepared_stmts[name]

    def get_model(self, model_name, default=None):
        return self._models.get(model_name, default)

    def get_models(self):
        return self._models.values()

    @property
    def model(self):
        return namedtuple('CassandraModels', self._models.keys())(*self._models.values())

        # @TODO
        # def bind_prepared_statement(self, name, value, consistency_level=ConsistencyLevel.ALL):


class Cassandra(DependencyProvider):
    def __init__(self, keyspace=None, models=list(), simple_keyspace=True, connection_name=DEFAULT_CONNECTION_NAME):
        self.keyspace = keyspace
        if not isinstance(models, (list, tuple)):
            models = [models]
        self.string_models = filter(lambda y: isinstance(y, tuple([text_type] + list(string_types))), models)
        self.class_models = filter(lambda x: inspect.isclass(x) and issubclass(x, cassandra_models.Model), models)
        self.connection_name = connection_name
        self.models = None
        self.connection = None
        self.cluster = None
        self.session = None
        self.keyspace_func = KEYSPACES_FUNCTIONS.get(
            SIMPLE_KEYSPACE if simple_keyspace else NETWORK_KEYSPACE,
            KEYSPACES_FUNCTIONS.get(SIMPLE_KEYSPACE)
        )

    def setup(self):
        if not self.keyspace or not isinstance(self.keyspace, text_type):
            self.keyspace = self.container.service_name
        has_models = any([self.string_models, self.class_models])
        hosts = os.environ.get(CASSANDRA_HOSTS, 'cassandra')
        cluster_options = dict(
            reconnection_policy=ExponentialReconnectionPolicy(base_delay=3, max_delay=15),
            connection_class=EventletConnection
        )
        if has_models:
            cluster_options['protocol_version'] = 3
            os.environ.setdefault('CQLENG_ALLOW_SCHEMA_MANAGEMENT', '1')

        hosts = map(lambda x: x.strip(), filter(None, self.container.config.get(CASSANDRA_HOSTS, hosts).split(',')))
        self.connection = register_connection(
            name=self.connection_name,
            hosts=hosts,
            lazy_connect=False,
            retry_connect=True,
            cluster_options=cluster_options,
            default=self.connection_name == DEFAULT_CONNECTION_NAME
        )
        self.session = get_session(self.connection_name)
        self.cluster = get_cluster(self.connection_name)

        if callable(self.keyspace_func):
            self.keyspace_func(self.keyspace, int(len(hosts) / 2) or 1)
        if self.session:
            self.session.set_keyspace(self.keyspace)
        if has_models:
            for model in self._resolve_models():
                sync_table(model, [self.keyspace], [self.connection])

    def _resolve_models(self):
        self.models = filter(
            lambda x: issubclass(x, cassandra_models.Model),
            filter(None, map(lambda m: import_string(m, silent=True), self.string_models) + self.class_models)
        )
        return self.models

    def get_dependency(self, worker_ctx):
        try:
            self.cluster.refresh_nodes()
        except Exception as e:
            pass
        return CassandraHelper(self.keyspace, self.connection, self.cluster, self.session, self.models)

    def stop(self):
        try:
            unregister_connection(self.connection_name)
        except Exception as e:
            self.cluster.shutdown()
        self.connection = None
        self.cluster = None
        self.session = None

    def kill(self):
        self.stop()
