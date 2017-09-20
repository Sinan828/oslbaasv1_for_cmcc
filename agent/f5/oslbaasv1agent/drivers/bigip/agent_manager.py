# Copyright 2014 F5 Networks Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import uuid
import datetime
import copy
from oslo.config import cfg  # @UnresolvedImport
from neutron.agent import rpc as agent_rpc
from neutron.common import constants as neutron_constants
from neutron import context
from neutron.common import log
from neutron.common import topics
from neutron.common.exceptions import NeutronException
from neutron.openstack.common import loopingcall
from neutron.openstack.common import periodic_task

from f5.oslbaasv1agent.drivers.bigip import agent_api
from f5.oslbaasv1agent.drivers.bigip import constants
import f5.oslbaasv1agent.drivers.bigip.constants as lbaasv1constants

preJuno = False
preKilo = False

try:
    from neutron.openstack.common import importutils
    from neutron.openstack.common import log as logging
    preKilo = True
    try:
        from neutron.openstack.common.rpc import dispatcher
        preJuno = True
    except ImportError:
        from neutron.common import rpc
except ImportError:
    from oslo_log import log as logging
    from oslo_utils import importutils
    from f5.oslbaasv1agent.drivers.bigip import rpc

LOG = logging.getLogger(__name__)

__VERSION__ = "0.1.1"

# configuration options useful to all drivers
OPTS = [
    cfg.IntOpt(
        'periodic_interval',
        default=10,
        help='Seconds between periodic task runs'
    ),
    cfg.BoolOpt(
        'start_agent_admin_state_up',
        default=True,
        help='Should the agent force its admin_state_up to True on boot'
    ),
    cfg.StrOpt(
        'f5_bigip_lbaas_device_driver',
        default=('f5.oslbaasv1agent.drivers.bigip'
                 '.icontrol_driver.iControlDriver'),
        help=_('The driver used to provision BigIPs'),
    ),
    cfg.BoolOpt(
        'l2_population',
        default=False,
        help=_('Use L2 Populate service for fdb entries on the BIG-IP')
    ),
    cfg.BoolOpt(
        'f5_global_routed_mode',
        default=False,
        help=_('Disable all L2 and L3 integration in favor or global routing')
    ),
    cfg.BoolOpt(
        'use_namespaces',
        default=True,
        help=_('Allow overlapping IP addresses for tenants')
    ),
    cfg.BoolOpt(
        'f5_snat_mode',
        default=True,
        help=_('use SNATs, not direct routed mode')
    ),
    cfg.IntOpt(
        'f5_snat_addresses_per_subnet',
        default='1',
        help=_('Interface and VLAN for the VTEP overlay network')
    ),
    cfg.StrOpt(
        'agent_id',
        default=None,
        help=('static agent ID to use with Neutron')
    ),
    cfg.StrOpt(
        'static_agent_configuration_data',
        default=None,
        help=_(
            'static name:value entries to add to the agent configurations')
    ),
    cfg.IntOpt(
        'service_resync_interval',
        default=300,
        help=_('Number of seconds between service refresh check')
    ),
    cfg.StrOpt(
        'environment_prefix', default='',
        help=_('The object name prefix for this environment'),
    ),
    cfg.BoolOpt(
        'environment_specific_plugin', default=False,
        help=_('Use environment specific plugin topic')
    ),
    cfg.IntOpt(
        'environment_group_number',
        default=1,
        help=_('Agent group number for it environment')
    ),
    cfg.DictOpt(
        'capacity_policy', default={},
        help=_('Metrics to measure capacity and their limits.')
    ),
    cfg.IntOpt(
        'f5_pending_services_timeout',
        default=60,
        help=(
            'Amount of time to wait for a pending service to become active')
    ),
    cfg.IntOpt(
        'f5_errored_services_timeout',
        default=60,
        help=(
            'Amount of time to wait for a errored service to become active')
    )
]

PERIODIC_TASK_INTERVAL = 10

class LogicalServiceCache(object):
    """Manage a cache of known services."""

    class Service(object):
        """Inner classes used to hold values for weakref lookups."""
        def __init__(self, port_id, pool_id, tenant_id, agent_host):
            self.port_id = port_id
            self.pool_id = pool_id
            self.tenant_id = tenant_id
            self.agent_host = agent_host

        def __eq__(self, other):
            return self.__dict__ == other.__dict__

        def __hash__(self):
            return hash(
                (self.port_id,
                 self.pool_id,
                 self.tenant_id,
                 self.agent_host)
            )

    def __init__(self):
        LOG.debug(_("Initializing LogicalServiceCache version %s"
                    % __VERSION__))
        self.services = {}

    @property
    def size(self):
        return len(self.services)

    def put(self, service, agent_host):
        if 'port_id' in service['vip']:
            port_id = service['vip']['port_id']
        else:
            port_id = None
        pool_id = service['pool']['id']
        tenant_id = service['pool']['tenant_id']
        if pool_id not in self.services:
            s = self.Service(port_id, pool_id, tenant_id, agent_host)
            self.services[pool_id] = s
        else:
            s = self.services[pool_id]
            s.tenant_id = tenant_id
            s.port_id = port_id
            s.agent_host = agent_host

    def remove(self, service):
        if not isinstance(service, self.Service):
            pool_id = service['pool']['id']
        else:
            pool_id = service.pool_id
        if pool_id in self.services:
            del(self.services[pool_id])

    def remove_by_pool_id(self, pool_id):
        if pool_id in self.services:
            del(self.services[pool_id])

    def get_by_pool_id(self, pool_id):
        if pool_id in self.services:
            return self.services[pool_id]
        else:
            return None

    def get_pool_ids(self):
        return self.services.keys()

    def get_tenant_ids(self):
        tenant_ids = {}
        for service in self.services:
            tenant_ids[service.tenant_id] = 1
        return tenant_ids.keys()

    def get_agent_hosts(self):
        agent_hosts = {}
        for service in self.services:
            agent_hosts[service.agent_host] = 1
        return agent_hosts.keys()


class LbaasAgentManagerBase(periodic_task.PeriodicTasks):

    # history
    #   1.0 Initial version
    #   1.1 Support agent_updated call
    RPC_API_VERSION = '1.1'

    # Not using __init__ in order to avoid complexities with super().
    # See derived classes after this class
    def do_init(self, conf):
        LOG.info(_('Initializing LbaasAgentManager'))
        self.conf = conf
        self.context = context.get_admin_context_without_session()
        self.serializer = None
        global PERIODIC_TASK_INTERVAL
        PERIODIC_TASK_INTERVAL = self.conf.periodic_interval

        # create the cache of provisioned services
        self.cache = LogicalServiceCache()
        self.last_resync = datetime.datetime.now()
        self.needs_resync = False
        self.plugin_rpc = None
        self.tunnel_rpc = None
        self.l2_pop_rpc = None
        self.state_rpc = None
        self.pending_services = {}

        if conf.service_resync_interval:
            self.service_resync_interval = conf.service_resync_interval
        else:
            self.service_resync_interval = constants.RESYNC_INTERVAL
        LOG.debug(_('setting service resync interval to %d seconds'
                    % self.service_resync_interval))

        self.lbdriver = importutils.import_object(
            conf.f5_bigip_lbaas_device_driver, self.conf)

        # try:
        #     LOG.debug(_('loading LBaaS driver %s'
        #                 % conf.f5_bigip_lbaas_device_driver))
        #         self.lbdriver = importutils.import_object(
        #             conf.f5_bigip_lbaas_device_driver, self.conf)
        #     if self.lbdriver.agent_id:
        #         self.agent_host = conf.host + ":" + self.lbdriver.agent_id
        #         self.lbdriver.agent_host = self.agent_host
        #         LOG.debug('setting agent host to %s' % self.agent_host)
        #     else:
        #         self.agent_host = None
        #         LOG.error(_('Driver did not initialize. Fix the driver config '
        #                     'and restart the agent.'))
        #         return
        # except ImportError as ie:
        #     msg = _('Error importing loadbalancer device driver: %s error %s'
        #             % (conf.f5_bigip_lbaas_device_driver,  repr(ie)))
        #     LOG.error(msg)
        #     raise SystemExit(msg)

        # Set the agent ID
        if self.conf.agent_id:
            self.agent_host = self.conf.agent_id
            LOG.debug('setting agent host to %s' % self.agent_host)
        else:
            # If not set statically, add the driver agent env hash
            agent_hash = str(
                uuid.uuid5(uuid.NAMESPACE_DNS,
                           self.conf.environment_prefix +
                           '.' + self.lbdriver.hostnames[0])
                )
            self.agent_host = conf.host + ":" + agent_hash
            LOG.debug('setting agent host to %s' % self.agent_host)

        # agent_configurations = \
        #     {'environment_prefix': self.conf.environment_prefix,
        #      'environment_group_number': self.conf.environment_group_number,
        #      'global_routed_mode': self.conf.f5_global_routed_mode}
        agent_configurations = (
            {'environment_prefix': self.conf.environment_prefix,
             'environment_group_number': self.conf.environment_group_number,
             'global_routed_mode': self.conf.f5_global_routed_mode}
        )

        if self.conf.static_agent_configuration_data:
            entries = \
                str(self.conf.static_agent_configuration_data).split(',')
            for entry in entries:
                nv = entry.strip().split(':')
                if len(nv) > 1:
                    agent_configurations[nv[0]] = nv[1]

        self.admin_state_up = self.conf.start_agent_admin_state_up

        self.agent_state = {
            'binary': lbaasv1constants.AGENT_BINARY_NAME,
            'host': self.agent_host,
            'topic': lbaasv1constants.TOPIC_LOADBALANCER_AGENT,
            'agent_type': neutron_constants.AGENT_TYPE_LOADBALANCER,
            'l2_population': self.conf.l2_population,
            'configurations': agent_configurations,
            'start_flag': True}

        # Setup RPC for communications to and from controller
        self._setup_rpc()

        # Load the driver.
        self._load_driver(conf)

        # Set driver context for RPC.
        self.lbdriver.set_context(self.context)
        # Allow the driver to make callbacks to the LBaaS driver plugin
        self.lbdriver.set_plugin_rpc(self.plugin_rpc)
        # Allow the driver to update tunnel endpoints
        self.lbdriver.set_tunnel_rpc(self.tunnel_rpc)
        # Allow the driver to update forwarding records in the SDN
        self.lbdriver.set_l2pop_rpc(self.l2_pop_rpc)
        # Allow the driver to force and agent state report to the controller
        self.lbdriver.set_agent_report_state(self._report_state)

        # Set the flag to resync tunnels/services
        self.needs_resync = True

        # Mark this agent admin_state_up per startup policy
        if(self.admin_state_up):
            self.plugin_rpc.set_agent_admin_state(self.admin_state_up)

        # Start state reporting of agent to Neutron
        report_interval = self.conf.AGENT.report_interval
        if report_interval:
            heartbeat = loopingcall.FixedIntervalLoopingCall(
                self._report_state)
            heartbeat.start(interval=report_interval)

    def _load_driver(self, conf):
        self.lbdriver = None

        LOG.debug('loading LBaaS driver %s' %
                  conf.f5_bigip_lbaas_device_driver)
        try:
            self.lbdriver = importutils.import_object(
                conf.f5_bigip_lbaas_device_driver,
                self.conf)
            return
        except ImportError as ie:
            msg = ('Error importing loadbalancer device driver: %s error %s'
                   % (conf.f5_bigip_lbaas_device_driver, repr(ie)))
            LOG.error(msg)
            raise SystemExit(msg)

    @log.log
    def _setup_rpc(self):

        # LBaaS Callbacks API
        topic = lbaasv1constants.TOPIC_PROCESS_ON_HOST
        if self.conf.environment_specific_plugin:
            topic = topic + '_' + self.conf.environment_prefix
            LOG.debug('agent in %s environment will send callbacks to %s'
                      % (self.conf.environment_prefix, topic))
        self.plugin_rpc = agent_api.LbaasAgentApi(
            topic,
            self.context,
            self.conf.environment_prefix,
            self.conf.environment_group_number,
            self.agent_host
        )
        # Allow driver to make callbacks using the
        # same RPC proxy as the manager
        self.lbdriver.set_plugin_rpc(self.plugin_rpc)

        # Agent state Callbacks API
        self.state_rpc = agent_rpc.PluginReportStateAPI(topic)
        report_interval = self.conf.AGENT.report_interval
        if report_interval:
            heartbeat = loopingcall.FixedIntervalLoopingCall(
                self._report_state)
            heartbeat.start(interval=report_interval)

        # The LBaaS agent listener with it's host are registered
        # as part of the rpc.Service. Here we are setting up
        # other message queues to listen for updates from
        # Neutron.
        if not self.conf.f5_global_routed_mode:
            # Core plugin Callbacks API for tunnel updates
            self.lbdriver.set_tunnel_rpc(agent_api.CoreAgentApi(topics.PLUGIN))

            # L2 Populate plugin Callbacks API
            if self.conf.l2_population:
                self.lbdriver.set_l2pop_rpc(agent_api.L2PopulationApi())

            # Besides LBaaS Plugin calls... what else to consume

            # tunnel updates to support vxlan and gre endpoint
            # NOTE:  the driver can decide to handle endpoint
            # membership based on the rpc notification or through
            # other means (i.e. as part of a service definition)
            consumers = [[constants.TUNNEL, topics.UPDATE]]
            # L2 populate fdb calls
            # NOTE:  the driver can decide to handle fdb updates
            # or use some other mechanism (i.e. flooding) to
            # learn about port updates.
            if self.conf.l2_population:
                consumers.append(
                    [topics.L2POPULATION, topics.UPDATE, self.agent_host]
                )

            if preJuno:
                self.dispatcher = dispatcher.RpcDispatcher([self])
            else:
                self.endpoints = [self]

            LOG.debug(_('registering to %s consumer on RPC topic: %s'
                        % (consumers, topics.AGENT)))
            if preJuno:
                self.connection = agent_rpc.create_consumers(
                    self.dispatcher,
                    topics.AGENT,
                    consumers
                )
            else:
                self.connection = agent_rpc.create_consumers(
                    self.endpoints,
                    topics.AGENT,
                    consumers
                )

    def _report_state(self, force_resync=False):
        try:
            if force_resync:
                self.needs_resync = True
                self.cache.services = {}
                self.lbdriver.flush_cache()
            # use the admin_state_up to notify the
            # controller if all backend devices
            # are functioning properly. If not
            # automatically set the admin_state_up
            # for this agent to False
            if self.lbdriver:
                if not self.lbdriver.backend_integrity():
                    self.needs_resync = True
                    self.cache.services = {}
                    self.needs_resync = True
                    self.lbdriver.flush_cache()
                    self.plugin_rpc.set_agent_admin_state(False)
                    self.admin_state_up = False
                else:
                    # if we are transitioning from down to up,
                    # change the controller state for this agent
                    if not self.admin_state_up:
                        self.plugin_rpc.set_agent_admin_state(True)
                        self.admin_state_up = True

            if self.lbdriver:
                self.agent_state['configurations'].update(
                    self.lbdriver.get_agent_configurations()
                )

            # add the capacity score, used by the scheduler
            # for horizontal scaling of an environment, from
            # the driver
            if self.conf.capacity_policy:
                env_score = (
                    self.lbdriver.generate_capacity_score(
                        self.conf.capacity_policy
                    )
                )
                self.agent_state['configurations'][
                    'environment_capaciy_score'] = env_score
            else:
                self.agent_state['configurations'][
                    'environment_capacity_score'] = 0

            LOG.debug("reporting state of agent as: %s" % self.agent_state)
            self.state_rpc.report_state(self.context, self.agent_state)
            self.agent_state.pop('start_flag', None)

        except Exception as e:
            LOG.exception(("Failed to report state: " + str(e.message)))

    def initialize_service_hook(self, started_by):
        # Prior to Juno.2, multiple listeners were created, including
        # topic.host, but that was removed. We manually restore that
        # listener here if we see only one topic listener on this connection.
        if hasattr(started_by.conn, 'servers') and \
                len(started_by.conn.servers) == 1:
            node_topic = '%s.%s' % (started_by.topic, started_by.host)
            LOG.debug("Listening on rpc topic %s" % node_topic)
            endpoints = [started_by.manager]
            started_by.conn.create_consumer(
                node_topic, endpoints, fanout=False)
        self.sync_state()

    @periodic_task.periodic_task(spacing=PERIODIC_TASK_INTERVAL)
    def connect_driver(self, context):
        """Trigger driver connect attempts to all devices"""
        if self.lbdriver:
            self.lbdriver.connect()

    @periodic_task.periodic_task(spacing=PERIODIC_TASK_INTERVAL)
    def recover_errored_devices(self, context):
        """Try to reconnect to errored devices."""
        if self.lbdriver:
            LOG.debug("running periodic task to retry errored devices")
            self.lbdriver.recover_errored_devices()

    @periodic_task.periodic_task
    def periodic_resync(self, context):
        LOG.debug("tunnel_sync: periodic_resync called")
        now = datetime.datetime.now()
        # check if a resync has not been requested by the driver
        if not self.needs_resync:
            # check if we hit the resync interval
            if (now - self.last_resync).seconds > self.service_resync_interval:
                self.needs_resync = True
                LOG.debug(
                    'forcing resync of services on resync timer (%d seconds).'
                    % self.service_resync_interval)
                self.cache.services = {}
                self.last_resync = now
                self.lbdriver.flush_cache()
                LOG.debug("periodic_sync: service_resync_interval expired: %s"
                          % str(self.needs_resync))
        # resync if we need to
        if self.needs_resync:
            LOG.debug("resync required at: %s" % now)
            self.needs_resync = False
            # advertise devices as VTEPs if required
            if self.tunnel_sync():
                self.needs_resync = True
            # synchronize LBaaS objects from controller
            if self.sync_state():
                self.needs_resync = True
            # clean any objects orphaned on devices and persist configs
            # if self.clean_orphaned_objects_and_save_device_config():
            #     self.needs_resync = True

    @periodic_task.periodic_task(spacing=30)
    def collect_stats(self, context):
        if not self.plugin_rpc:
            return
        pool_services = copy.deepcopy(self.cache.services)
        for pool_id in pool_services:
            service = pool_services[pool_id]
            if self.agent_host == service.agent_host:
                try:
                    LOG.debug("collecting stats for pool %s" % service.pool_id)
                    stats = self.lbdriver.get_stats(
                        self.plugin_rpc.get_service_by_pool_id(
                            service.pool_id,
                            self.conf.f5_global_routed_mode
                        )
                    )
                    if stats:
                        self.plugin_rpc.update_pool_stats(service.pool_id,
                                                          stats)
                except Exception as e:
                    LOG.exception(_('Error upating stats' + str(e.message)))
                    self.needs_resync = True

    @periodic_task.periodic_task(spacing=600)
    def backup_configuration(self, context):
        self.lbdriver.backup_configuration()

    def tunnel_sync(self):
        LOG.debug("manager:tunnel_sync: calling driver tunnel_sync")
        return self.lbdriver.tunnel_sync()

    def sync_state(self):
        """Synchronize device configuration from controller state."""
        resync = False

        if hasattr(self, 'lbdriver'):
            if not self.lbdriver.backend_integrity():
                return resync

        known_services = set()
        owned_services = set()
        for pool_id, service in self.cache.services.iteritems():
            known_services.add(pool_id)
            if self.agent_host == service.agent_host:
                owned_services.add(pool_id)
        now = datetime.datetime.now()

        try:
            # Get pools from the environment which are bound to
            # this agent.
            active_pools = (
                self.plugin_rpc.get_active_pools(host=self.agent_host)
            )
            active_pool_ids = set(
                [pool['pool_id'] for pool in active_pools]
            )

            LOG.debug("plugin produced the list of active pool ids: %s"
                      % list(active_pool_ids))
            LOG.debug("currently known pool ids before sync are: %s"
                      % list(known_services))

            # Validate each service we own, i.e. loadbalancers to which this
            # agent is bound, that does not exist in our service cache.
            for pool_id in active_pool_ids:
                if not self.cache.get_by_pool_id(pool_id):
                    self.validate_service(pool_id)

            errored_pools = (
                self.plugin_rpc.get_errored_pools(host=self.agent_host)
            )
            errored_pool_ids = set(
                [pool['pool_id'] for pool in errored_pools]
            )

            LOG.debug(
                "plugin produced the list of errored pool ids: %s"
                % list(errored_pool_ids))
            LOG.debug("currently known pool ids before sync are: %s"
                      % list(known_services))

            # Validate each service we own, i.e. pools to which this
            # agent is bound, that does not exist in our service cache.
            for pool_id in errored_pool_ids:
                if not self.cache.get_by_pool_id(pool_id):
                    self.validate_service(pool_id)

            # This produces a list of loadbalancers with pending tasks to
            # be performed.
            pending_pools = (
                self.plugin_rpc.get_pending_pools(host=self.agent_host)
            )
            pending_pool_ids = set(
                [pool['pool_id'] for pool in pending_pools]
            )
            LOG.debug(
                "plugin produced the list of pending pool ids: %s"
                % list(pending_pool_ids))

            for pool_id in pending_pool_ids:
                pool_pending = self.refresh_service(pool_id)
                if pool_pending:
                    if pool_id not in self.pending_services:
                        self.pending_services[pool_id] = now

                    time_added = self.pending_services[pool_id]
                    time_expired = ((now - time_added).seconds >
                                    self.conf.f5_pending_services_timeout)

                    if time_expired:
                        pool_pending = False
                        self.service_timeout(pool_id)

                if not pool_pending:
                    del self.pending_services[pool_id]

            # If there are services in the pending cache resync
            if self.pending_services:
                resync = True

            # Get a list of any cached service we now know after
            # refreshing services
            known_services = set()
            for (pool_id, service) in self.cache.services.iteritems():
                if self.agent_host == service.agent_host:
                    known_services.add(pool_id)
            LOG.debug("currently known pool ids after sync: %s"
                      % list(known_services))

        except Exception as e:
            LOG.error("Unable to sync state: %s" % e.message)
            resync = True

        return resync

    @log.log
    def validate_service(self, pool_id):
        if not self.plugin_rpc:
            return
        try:
            service = self.plugin_rpc.get_service_by_pool_id(
                pool_id,
                self.conf.f5_global_routed_mode
            )
            self.cache.put(service, self.agent_host)
            if not self.lbdriver.exists(service):
                LOG.error(_('active pool %s is not on BIG-IP.. syncing'
                            % pool_id))
                self.lbdriver.sync(service)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as e:
            # the pool may have been deleted in the moment
            # between getting the list of pools and validation
            active_pool_ids = set()
            for active_pool in self.plugin_rpc.get_active_pools():
                if self.agent_host == active_pool['agent_host']:
                    active_pool_ids.add(active_pool['pool_id'])
            if pool_id in active_pool_ids:
                LOG.exception(_('Unable to validate service for pool: %s' +
                                str(e.message)), pool_id)

    @log.log
    def refresh_service(self, pool_id):
        if not self.plugin_rpc:
            return
        try:
            service = self.plugin_rpc.get_service_by_pool_id(
                pool_id,
                self.conf.f5_global_routed_mode
            )
            self.cache.put(service, self.agent_host)
            self.lbdriver.sync(service)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)
            self.needs_resync = True

    @log.log
    def service_timeout(self, pool_id):
        try:
            service = self.plugin_rpc.get_service_by_pool_id(
                pool_id
            )
            self.cache.put(service, self.agent_host)
            self.lbdriver.update_service_status(service, timed_out=True)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as e:
            LOG.error("Exception: %s" % e.message)

    @log.log
    def destroy_service(self, pool_id):
        if not self.plugin_rpc:
            return
        service = self.plugin_rpc.get_service_by_pool_id(
            pool_id,
            self.conf.f5_global_routed_mode
        )
        if not service:
            return
        try:
            self.lbdriver.delete_pool(pool_id, service)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)
            self.needs_resync = True
        self.cache.remove_by_pool_id(pool_id)

    @log.log
    def clean_orphaned_objects_and_save_device_config(self):

        cleaned = False

        try:
            #
            # Global cluster refresh tasks
            #

            global_agent = self.plugin_rpc.get_clusterwide_agent(
                self.conf.environment_prefix,
                self.conf.environment_group_number
            )

            if 'host' not in global_agent:
                LOG.debug('No global agent available to sync config')
                return True

            if global_agent['host'] == self.agent_host:
                LOG.debug('this agent is the global config agent')
                # We're the global agent perform global cluster tasks

                # There are two independent types of service objects
                # the LBaaS implments: 1) loadbalancers + 2) pools
                # We will first try to find any orphaned pools
                # and remove them.

                # Ask BIG-IP for all deployed loadbalancers (virtual addresses)
                lbs = self.lbdriver.get_all_deployed_loadbalancers(
                    purge_orphaned_folders=True)
                if lbs:
                    # Ask Neutron for the status of each deployed loadbalancer
                    lbs_status = self.plugin_rpc.validate_loadbalancers_state(
                        list(lbs.keys()))
                    LOG.debug('validate_loadbalancers_state returned: %s'
                              % lbs_status)
                    lbs_removed = False
                    for lbid in lbs_status:
                        # If the statu is Unknown, it no longer exists
                        # in Neutron and thus should be removed from the BIG-IP
                        if lbs_status[lbid] in ['Unknown']:
                            LOG.debug('removing orphaned loadbalancer %s'
                                      % lbid)
                            # This will remove pools, virtual servers and
                            # virtual addresses
                            self.lbdriver.purge_orphaned_loadbalancer(
                                tenant_id=lbs[lbid]['tenant_id'],
                                loadbalancer_id=lbid,
                                hostnames=lbs[lbid]['hostnames'])
                            lbs_removed = True
                    if lbs_removed:
                        # If we have removed load balancers, then scrub
                        # for tenant folders we can delete because they
                        # no longer contain loadbalancers.
                        self.lbdriver.get_all_deployed_loadbalancers(
                            purge_orphaned_folders=True)

                # Ask the BIG-IP for all deployed pools not associated
                # to a virtual server
                pools = self.lbdriver.get_all_deployed_pools()
                if pools:
                    # Ask Neutron for the status of all deployed pools
                    pools_status = self.plugin_rpc.validate_pools_state(
                        list(pools.keys()))
                    LOG.debug('validated_pools_state returned: %s'
                              % pools_status)
                    for poolid in pools_status:
                        # If the pool status is Unknown, it no longer exists
                        # in Neutron and thus should be removed from BIG-IP
                        if pools_status[poolid] in ['Unknown']:
                            LOG.debug('removing orphaned pool %s' % poolid)
                            self.lbdriver.purge_orphaned_pool(
                                tenant_id=pools[poolid]['tenant_id'],
                                pool_id=poolid,
                                hostnames=pools[poolid]['hostnames'])
            else:
                LOG.debug('the global agent is %s' % (global_agent['host']))
            # serialize config and save to disk
            self.lbdriver.backup_configuration()
        except Exception as e:
            LOG.error("Unable to sync state: %s" % e.message)
            cleaned = True

        return cleaned

    @log.log
    def remove_orphans(self, all_pools):
        try:
            self.lbdriver.remove_orphans(all_pools)
        except NotImplementedError:
            pass  # Not all drivers will support this

    @log.log
    def reload_pool(self, context, pool_id=None, host=None):
        """Handle RPC cast from plugin to reload a pool."""
        if host and host == self.agent_host:
            if pool_id:
                self.refresh_service(pool_id)

    @log.log
    def get_pool_stats(self, context, pool, service):
        LOG.debug("agent_manager got get_pool_stats call")
        if not self.plugin_rpc:
            return
        try:
            LOG.debug("agent_manager calling driver get_stats")
            stats = self.lbdriver.get_stats(service)
            LOG.debug("agent_manager called driver get_stats")
            if stats:
                    LOG.debug("agent_manager calling update_pool_stats")
                    self.plugin_rpc.update_pool_stats(pool['id'], stats)
                    LOG.debug("agent_manager called update_pool_stats")
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)

    @log.log
    def create_vip(self, context, vip, service):
        """Handle RPC cast from plugin to create_vip"""
        try:
            self.lbdriver.create_vip(vip, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)

    @log.log
    def update_vip(self, context, old_vip, vip, service):
        """Handle RPC cast from plugin to update_vip"""
        try:
            self.lbdriver.update_vip(old_vip, vip, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)

    @log.log
    def delete_vip(self, context, vip, service):
        """Handle RPC cast from plugin to delete_vip"""
        try:
            self.lbdriver.delete_vip(vip, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)

    @log.log
    def create_pool(self, context, pool, service):
        """Handle RPC cast from plugin to create_pool"""
        try:
            self.lbdriver.create_pool(pool, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)

    @log.log
    def update_pool(self, context, old_pool, pool, service):
        """Handle RPC cast from plugin to update_pool"""
        try:
            self.lbdriver.update_pool(old_pool, pool, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("Exception: %s" % exc.message)

    @log.log
    def delete_pool(self, context, pool, service):
        """Handle RPC cast from plugin to delete_pool"""
        try:
            self.lbdriver.delete_pool(pool, service)
            self.cache.remove_by_pool_id(pool['id'])
        except NeutronException as exc:
            LOG.error("delete_pool: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("delete_pool: Exception: %s" % exc.message)

    @log.log
    def create_member(self, context, member, service):
        """Handle RPC cast from plugin to create_member"""
        try:
            self.lbdriver.create_member(member, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("create_member: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("create_member: Exception: %s" % exc.message)

    @log.log
    def update_member(self, context, old_member, member, service):
        """Handle RPC cast from plugin to update_member"""
        try:
            self.lbdriver.update_member(old_member, member, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("update_member: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("update_member: Exception: %s" % exc.message)

    @log.log
    def delete_member(self, context, member, service):
        """Handle RPC cast from plugin to delete_member"""
        try:
            self.lbdriver.delete_member(member, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("delete_member: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("delete_member: Exception: %s" % exc.message)

    @log.log
    def create_pool_health_monitor(self, context, health_monitor,
                                   pool, service):
        """Handle RPC cast from plugin to create_pool_health_monitor"""
        try:
            self.lbdriver.create_pool_health_monitor(health_monitor,
                                                     pool, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error(_("create_pool_health_monitor: NeutronException: %s"
                        % exc.msg))
        except Exception as exc:
            LOG.error(_("create_pool_health_monitor: Exception: %s"
                        % exc.message))

    @log.log
    def update_health_monitor(self, context, old_health_monitor,
                              health_monitor, pool, service):
        """Handle RPC cast from plugin to update_health_monitor"""
        try:
            self.lbdriver.update_health_monitor(old_health_monitor,
                                                health_monitor,
                                                pool, service)
            self.cache.put(service, self.agent_host)
        except NeutronException as exc:
            LOG.error("update_health_monitor: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("update_health_monitor: Exception: %s" % exc.message)

    @log.log
    def delete_pool_health_monitor(self, context, health_monitor,
                                   pool, service):
        """Handle RPC cast from plugin to delete_pool_health_monitor"""
        try:
            self.lbdriver.delete_pool_health_monitor(health_monitor,
                                                     pool, service)
        except NeutronException as exc:
            LOG.error(_("delete_pool_health_monitor: NeutronException: %s"
                        % exc.msg))
        except Exception as exc:
            LOG.error(_("delete_pool_health_monitor: Exception: %s"
                      % exc.message))

    @log.log
    def agent_updated(self, context, payload):
        """Handle the agent_updated notification event."""
        if payload['admin_state_up'] != self.admin_state_up:
            self.admin_state_up = payload['admin_state_up']
            if self.admin_state_up:
                self.needs_resync = True
            else:
                for pool_id in self.cache.get_pool_ids():
                    self.destroy_service(pool_id)
            LOG.info(_("agent_updated by server side %s!"), payload)

    @log.log
    def tunnel_update(self, context, **kwargs):
        """Handle RPC cast from core to update tunnel definitions"""
        try:
            LOG.debug(_('received tunnel_update: %s' % kwargs))
            self.lbdriver.tunnel_update(**kwargs)
        except NeutronException as exc:
            LOG.error("tunnel_update: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("tunnel_update: Exception: %s" % exc.message)

    @log.log
    def add_fdb_entries(self, context, fdb_entries, host=None):
        """Handle RPC cast from core to update tunnel definitions"""
        try:
            LOG.debug(_('received add_fdb_entries: %s host: %s'
                        % (fdb_entries, host)))
            self.lbdriver.fdb_add(fdb_entries)
        except NeutronException as exc:
            LOG.error("fdb_add: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("fdb_add: Exception: %s" % exc.message)

    @log.log
    def remove_fdb_entries(self, context, fdb_entries, host=None):
        """Handle RPC cast from core to update tunnel definitions"""
        try:
            LOG.debug(_('received remove_fdb_entries: %s host: %s'
                        % (fdb_entries, host)))
            self.lbdriver.fdb_remove(fdb_entries)
        except NeutronException as exc:
            LOG.error("remove_fdb_entries: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("remove_fdb_entries: Exception: %s" % exc.message)

    @log.log
    def update_fdb_entries(self, context, fdb_entries, host=None):
        """Handle RPC cast from core to update tunnel definitions"""
        try:
            LOG.debug(_('received update_fdb_entries: %s host: %s'
                        % (fdb_entries, host)))
            self.lbdriver.fdb_update(fdb_entries)
        except NeutronException as exc:
            LOG.error("update_fdb_entrie: NeutronException: %s" % exc.msg)
        except Exception as exc:
            LOG.error("update_fdb_entrie: Exception: %s" % exc.message)

if preJuno:
    class LbaasAgentManager(LbaasAgentManagerBase):
        RPC_API_VERSION = '1.1'

        def __init__(self, conf):
            LbaasAgentManagerBase.do_init(self, conf)
else:
    class LbaasAgentManager(rpc.RpcCallback,
                            LbaasAgentManagerBase):  # @UndefinedVariable
        RPC_API_VERSION = '1.1'

        def __init__(self, conf):
            super(LbaasAgentManager, self).__init__()
            LbaasAgentManagerBase.do_init(self, conf)
