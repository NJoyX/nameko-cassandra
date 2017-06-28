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
from werkzeug.utils import import_string, cached_property

__author__ = 'Fill Q'
__all__ = ['Cassandra']

DEFAULT_CONNECTION_NAME = 'default'
CASSANDRA_VAR = 'CASSANDRA'
CASSANDRA_ENV_HOSTS_VAR = 'CASSANDRA_HOSTS'
SIMPLE_KEYSPACE = 'simple'
NETWORK_KEYSPACE = 'network'

""" 
Config sample:
------ yaml format -------

CASSANDRA:
  simple_topology:
    hosts:
      - node1
      - node2
    topology: simple
    OPTIONS:
      durable_writes: true
      replication_factor: 1

---------- or -----------

CASSANDRA:
  network_topology:
    hosts:
      - node1
      - node2
    topology: network
    OPTIONS:
      durable_writes: true
      dc_replication_map:
        dc1: 2
        dc2: 3
        dc3: 1
"""


class Cassandra(DependencyProvider):
    connection = None

    def __init__(self, keyspace=None, models=None, connection_name=DEFAULT_CONNECTION_NAME):
        if models is None:
            models = list()
        if not isinstance(models, (list, tuple)):
            models = [models]
        self.string_models = filter(lambda y: isinstance(y, tuple([text_type] + list(string_types))), models)
        self.class_models = filter(lambda x: inspect.isclass(x) and issubclass(x, cassandra_models.Model), models)

        self.keyspace = keyspace
        self.connection_name = connection_name

    @property
    def session(self):
        if self.connection:
            return get_session(self.connection_name)
        return

    @property
    def cluster(self):
        if self.connection:
            return get_cluster(self.connection_name)
        return

    @cached_property
    def models(self):
        return filter(
            lambda x: issubclass(x, cassandra_models.Model),
            filter(None, map(lambda m: import_string(m, silent=True), self.string_models) + self.class_models)
        )

    @property
    def cassandra_config(self):
        return self.container.config.get(CASSANDRA_VAR, {}).get(self.connection_name, {}) or {}

    @cached_property
    def nodes(self):
        hosts = self.cassandra_config.get('hosts')
        if hosts is None:
            hosts = os.environ.get(CASSANDRA_ENV_HOSTS_VAR, 'cassandra')
            return map(lambda x: x.strip(), filter(None, hosts.split(',')))
        return hosts

    def topology_options(self, topology):
        options = self.cassandra_config.get('OPTIONS', {})
        kwargs = dict(
            name=self.keyspace,
            connections=[self.connection_name],
            durable_writes=options.get('durable_writes', True))
        if topology == SIMPLE_KEYSPACE:
            _hosts_count = len(self.nodes)
            _replication_factor = int(round(_hosts_count / 2)) or 1
            kwargs['replication_factor'] = options.get('replication_factor', _replication_factor)
        elif topology == NETWORK_KEYSPACE:
            kwargs['dc_replication_map'] = options.get('dc_replication_map', {})
        return kwargs

    def setup(self):
        if not self.keyspace or not isinstance(self.keyspace, text_type):
            self.keyspace = self.container.service_name

        cluster_options = dict(
            reconnection_policy=ExponentialReconnectionPolicy(base_delay=3, max_delay=15),
            connection_class=EventletConnection
        )
        if self.models:
            cluster_options['protocol_version'] = 3
            os.environ.setdefault('CQLENG_ALLOW_SCHEMA_MANAGEMENT', '1')

        self.connection = register_connection(
            name=self.connection_name,
            hosts=self.nodes,
            lazy_connect=False,
            retry_connect=True,
            cluster_options=cluster_options,
            default=self.connection_name == DEFAULT_CONNECTION_NAME
        )

        topology = self.cassandra_config.get('topology', SIMPLE_KEYSPACE)
        if topology not in (SIMPLE_KEYSPACE, NETWORK_KEYSPACE):
            topology = SIMPLE_KEYSPACE

        topology_func = {
            SIMPLE_KEYSPACE: management.create_keyspace_simple,
            NETWORK_KEYSPACE: management.create_keyspace_network_topology
        }.get(topology)
        topology_func(**self.topology_options(topology))

        if self.session:
            self.session.set_keyspace(self.keyspace)

        for model in self.models:
            sync_table(model, [self.keyspace], [self.connection])

    def get_dependency(self, worker_ctx):
        try:
            self.cluster.refresh_nodes()
        except Exception as e:
            pass
        return CassandraHelper(self)

    def stop(self):
        try:
            unregister_connection(self.connection_name)
        except Exception as e:
            self.cluster.shutdown()
        self.connection = None

    def kill(self):
        self.stop()


class CassandraHelper(namedtuple('CassandraHelper', 'keyspace connection cluster session')):
    _prepared_stmts = weakref.WeakValueDictionary()
    _models = dict()

    def __init__(self, provider):
        super(CassandraHelper, self).__init__(**dict(
            keyspace=provider.keyspace,
            connection=provider.connection,
            cluster=provider.cluster,
            session=provider.session
        ))
        for model in provider.models:
            self._models[model.__name__] = model

    def register_user_type(self, name, obj):
        return self.cluster.register_user_type(self.keyspace, name, obj)

    def prepared_statement(self, name, query, consistency_level=ConsistencyLevel.QUORUM, force=False):
        if name in self._prepared_stmts and not force:
            return self._prepared_stmts[name]
        self._prepared_stmts[name] = self.session.prepare(query)
        if consistency_level in ConsistencyLevel.value_to_name:
            self._prepared_stmts[name].consistency_level = consistency_level
        return self._prepared_stmts[name]

    @cached_property
    def model(self):
        return namedtuple('CassandraModels', self._models.keys())(*self._models.values())

    # @TODO
    # def bind_prepared_statement(self, name, value, consistency_level=ConsistencyLevel.ALL):
