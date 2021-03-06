""" F5 Networks LBaaS Driver using iControl API of BIG-IP """
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
# pylint: disable=broad-except,star-args,no-self-use

from oslo.config import cfg  # @UnresolvedImport

try:
    from neutron.openstack.common import log as logging
    from neutron.openstack.common import importutils
    from neutron.services.loadbalancer import constants as lb_const
except ImportError:
    # Kilo
    from oslo_log import log as logging
    from oslo_utils import importutils
    from neutron_lbaas.services.loadbalancer import constants as lb_const

from neutron.plugins.common import constants as plugin_const
from neutron.common.exceptions import NeutronException, \
    InvalidConfigurationOption

from f5.oslbaasv1agent.drivers.bigip.lbaas_driver import LBaaSBaseDriver
from f5.oslbaasv1agent.drivers.bigip.vcmp import VcmpManager
from f5.oslbaasv1agent.drivers.bigip.tenants import BigipTenantManager
from f5.oslbaasv1agent.drivers.bigip.fdb_connector_ml2 import FDBConnectorML2
from f5.oslbaasv1agent.drivers.bigip.l2 import BigipL2Manager
from f5.oslbaasv1agent.drivers.bigip.network_direct import NetworkBuilderDirect
import f5.oslbaasv1agent.drivers.bigip.lbaas_iapp as lbaas_iapp
from f5.oslbaasv1agent.drivers.bigip.lbaas_bigip \
    import LBaaSBuilderBigipObjects, LBaaSBuilderBigipIApp
from f5.oslbaasv1agent.drivers.bigip.lbaas_bigiq import LBaaSBuilderBigiqIApp
from f5.oslbaasv1agent.drivers.bigip.utils import serialized

from f5.bigip import bigip as f5_bigip
from f5.common import constants as f5const
from f5.bigip import exceptions as f5ex
from f5.bigip import interfaces as bigip_interfaces
from f5.bigip.interfaces import strip_domain_address

from eventlet import greenthread
import uuid
import urllib2
import datetime
import hashlib
from time import time
import logging as std_logging

LOG = logging.getLogger(__name__)
NS_PREFIX = 'qlbaas-'
__VERSION__ = '0.1.1'

# plugin_const.CREATED added in juno.  PLUGIN_CREATED_FLAG is used for
# backward compatibility
# pylint: disable=bare-except
try:
    PLUGIN_CREATED_FLAG = plugin_const.CREATED
except:
    PLUGIN_CREATED_FLAG = plugin_const.ACTIVE
# pylint: enable=bare-except

# configuration objects specific to iControl driver
OPTS = [
    cfg.StrOpt(
        'bigiq_hostname',
        help=_('The hostname (name or IP address) to use for the BIG-IQ host'),
    ),
    cfg.StrOpt(
        'bigiq_admin_username',
        default='admin',
        help=_('The admin username to use for BIG-IQ authentication'),
    ),
    cfg.StrOpt(
        'bigiq_admin_password',
        default='[Provide password in config file]',
        secret=True,
        help=_('The admin password to use for BIG-IQ authentication')
    ),
    cfg.StrOpt(
        'openstack_keystone_uri',
        default='http://192.0.2.248:5000/',
        help=_('The admin password to use for BIG-IQ authentication')
    ),
    cfg.StrOpt(
        'openstack_admin_username',
        default='admin',
        help=_('The admin username to use for authentication '
               'with the Keystone service'),
    ),
    cfg.StrOpt(
        'openstack_admin_password',
        default='[Provide password in config file]',
        secret=True,
        help=_('The admin password to use for authentication'
               ' with the Keystone service')
    ),
    cfg.StrOpt(
        'bigip_management_username',
        default='admin',
        help=_('The admin username that the BIG-IQ will use to manage '
               'discovered BIG-IPs'),
    ),
    cfg.StrOpt(
        'bigip_management_password',
        default='[Provide password in config file]',
        secret=True,
        help=_('The admin password that the BIG-IQ will use to manage '
               'discovered BIG-IPs')
    ),
    cfg.StrOpt(
        'f5_device_type', default='external',
        help=_('What type of device onboarding')
    ),
    cfg.StrOpt(
        'f5_ha_type', default='pair',
        help=_('Are we standalone, pair(active/standby), or scalen')
    ),
    cfg.ListOpt(
        'f5_external_physical_mappings', default=['default:1.1:True'],
        help=_('Mapping between Neutron physical_network to interfaces')
    ),
    cfg.StrOpt(
        'sync_mode', default='replication',
        help=_('The sync mechanism: autosync or replication'),
    ),
    cfg.StrOpt(
        'f5_sync_mode', default='replication',
        help=_('The sync mechanism: autosync or replication'),
    ),
    cfg.StrOpt(
        'f5_vtep_folder', default='Common',
        help=_('Folder for the VTEP SelfIP'),
    ),
    cfg.StrOpt(
        'f5_vtep_selfip_name', default=None,
        help=_('Name of the VTEP SelfIP'),
    ),
    cfg.ListOpt(
        'advertised_tunnel_types', default=['vxlan'],
        help=_('tunnel types which are advertised to other VTEPs'),
    ),
    cfg.BoolOpt(
        'f5_populate_static_arp', default=True,
        help=_('create static arp entries based on service entries'),
    ),
    cfg.StrOpt(
        'vlan_binding_driver',
        default=None,
        help=_('driver class for binding vlans to device ports'),
    ),
    cfg.StrOpt(
        'interface_port_static_mappings',
        default=None,
        help=_('JSON encoded static mapping of'
               'devices to list of '
               'interface and port_id')
    ),
    cfg.StrOpt(
        'l3_binding_driver',
        default=None,
        help=_('driver class for binding l3 address to l2 ports'),
    ),
    cfg.StrOpt(
        'l3_binding_static_mappings', default=None,
        help=_('JSON encoded static mapping of'
               'subnet_id to list of '
               'port_id, device_id list.')
    ),
    cfg.BoolOpt(
        'f5_route_domain_strictness', default=False,
        help=_('Strict route domain isolation'),
    ),
    cfg.BoolOpt(
        'f5_common_external_networks', default=True,
        help=_('Treat external networks as common')
    ),
    cfg.StrOpt(
        'icontrol_vcmp_hostname',
        help=_('The hostname (name or IP address) to use for vCMP Host '
               'iControl access'),
    ),
    cfg.StrOpt(
        'icontrol_hostname',
        help=_('The hostname (name or IP address) to use for iControl access'),
    ),
    cfg.StrOpt(
        'icontrol_username', default='admin',
        help=_('The username to use for iControl access'),
    ),
    cfg.StrOpt(
        'icontrol_password', default='admin', secret=True,
        help=_('The password to use for iControl access'),
    ),
    cfg.IntOpt(
        'icontrol_connection_timeout', default=30,
        help=_('How many seconds to timeout a connection to BIG-IP'),
    ),
    cfg.IntOpt(
        'icontrol_connection_retry_interval', default=10,
        help=_('How many seconds to wait between retry connection attempts'),
    ),
    cfg.DictOpt(
        'common_network_ids', default={},
        help=_('network uuid to existing Common networks mapping')
    ),
    cfg.StrOpt(
        'icontrol_config_mode', default='objects',
        help=_('Whether to use iapp or objects for bigip configuration'),
    ),
    cfg.IntOpt(
        'max_namespaces_per_tenant', default=1,
        help=_('How many routing tables the BIG-IP will allocate per tenant'
               ' in order to accommodate overlapping IP subnets'),
    ),
]


# def is_connected(method):
#     """Decorator to check we are connected before provisioning."""
#     def wrapper(*args, **kwargs):
#         """ Necessary wrapper """
#         instance = args[0]
#         if instance.connected:
#             try:
#                 return method(*args, **kwargs)
#             except IOError as ioe:
#                 LOG.error(_('IO Error detected: %s' % method.__name__))
#                 instance.connect_bigips()
#                 raise ioe
#         else:
#             LOG.error(_('Cannot execute %s. Not connected. Connecting.'
#                         % method.__name__))
#             instance.connect_bigips()
#     return wrapper

def is_operational(method):
    # Decorator to check we are operational before provisioning.
    def wrapper(*args, **kwargs):
        instance = args[0]
        if instance.operational:
            try:
                return method(*args, **kwargs)
            except IOError as ioe:
                LOG.error('IO Error detected: %s' % method.__name__)
                LOG.error(str(ioe))
                raise ioe
        else:
            LOG.error('Cannot execute %s. Not operational. Re-initializing.'
                      % method.__name__)
            instance._init_bigips()
    return wrapper


class iControlDriver(LBaaSBaseDriver):
    """F5 LBaaS Driver for BIG-IP using iControl"""

    def __init__(self, conf, registerOpts=True):
        """ The registerOpts parameter allows a test to
            turn off config option handling so that it can
            set the options manually instead. """
        super(iControlDriver, self).__init__(conf)
        self.conf = conf
        if registerOpts:
            self.conf.register_opts(OPTS)
        self.initialized = False
        self.hostnames = None
        self.device_type = conf.f5_device_type
        self.plugin_rpc = None
        self.agent_report_state = None
        self.operational = False
        self.driver_name = 'f5-lbaas-icontrol'

        # BIG-IP containers
        self.__bigips = {}
        self.__last_connect_attempt = None

        self.tg_initialized = False
        self.ha_validated = False

        self.__traffic_groups = []

        self.agent_configurations = {}
        self.agent_configurations['device_drivers'] = [ self.driver_name ]
        self.agent_configurations['icontrol_endpoints'] = {}

        self.vcmp_manager = None
        self.tenant_manager = None
        self.fdb_connector = None
        self.bigip_l2_manager = None
        self.vlan_binding = None
        self.l3_binding = None
        self.network_builder = None
        self.lbaas_builder_bigip_iapp = None
        self.lbaas_builder_bigip_objects = None
        self.lbaas_builder_bigiq_iapp = None

        try:
            # driver mode settings - GRM vs L2 adjacent
            if self.conf.f5_global_routed_mode:
                LOG.info('WARNING - f5_global_routed_mode enabled.'
                         ' There will be no L2 or L3 orchestration'
                         ' or tenant isolation provisioned. All vips'
                         ' and pool members must be routable through'
                         ' pre-provisioned SelfIPs.')
                self.conf.use_namespaces = False
                self.conf.f5_snat_mode = True
                self.conf.f5_snat_addresses_per_subnet = 0
                self.agent_configurations['tunnel_types'] = []
                self.agent_configurations['bridge_mappings'] = {}
            else:
                self.agent_configurations['tunnel_types'] = \
                    self.conf.advertised_tunnel_types
                for net_id in self.conf.common_network_ids:
                    LOG.debug('network %s will be mapped to /Common/%s'
                              % (net_id, self.conf.common_network_ids[net_id]))

                self.agent_configurations['common_networks'] = \
                    self.conf.common_network_ids

                if self.conf.environment_prefix:
                    LOG.debug(_('BIG-IP name prefix for this environment: %s' %
                                self.conf.environment_prefix))
                    bigip_interfaces.OBJ_PREFIX = \
                        self.conf.environment_prefix + '_'

                LOG.debug(_('Setting static ARP population to %s'
                            % self.conf.f5_populate_static_arp))
                self.agent_configurations['f5_common_external_networks'] = \
                    self.conf.f5_common_external_networks
                f5const.FDB_POPULATE_STATIC_ARP = self.conf.f5_populate_static_arp

            self._init_bigip_hostnames()
            self._init_bigip_managers()
            self.initialized = True
            LOG.debug('iControlDriver loaded successfully')
        except Exception as exc:
            LOG.error("exception in intializing driver %s" % str(exc))
            self._set_agent_status(False)

    def connect_bigips(self):
        try:
            self._init_bigips()
            if self.conf.f5_global_routed_mode:
                local_ips = []
            else:
                local_ips = self.network_builder.initialize_tunneling()
            self._init_agent_config(local_ips)
        except Exception as exc:
            LOG.error("exception in intializing communications to BIG-IPs %s"
                      % str(exc))
            self._set_agent_status(False)

    def post_init(self):
        """ Run and Post Initialization Tasks """
        # run any post initialized tasks, now that the agent
        # is fully connected
        if self.vlan_binding:
            LOG.debug(
                'Getting BIG-IP device interface for VLAN Binding')
            self.vlan_binding.register_bigip_interfaces()
        if self.l3_binding:
            LOG.debug('Getting BIG-IP MAC Address for L3 Binding')
            self.l3_binding.register_bigip_mac_addresses()

    def connect(self):
        # initialize communications wiht BIG-IP via iControl
        try:
            self._init_bigips()
        except Exception as exc:
            LOG.error("exception in intializing communications to BIG-IPs %s"
                      % str(exc))
            self._set_agent_status(False)

    def _init_bigip_managers(self):
        """ Setup the managers that create big-ip configurations. """
        #self.vcmp_manager = VcmpManager(self)
        self.tenant_manager = BigipTenantManager(
            self.conf, self)

        if self.conf.vlan_binding_driver:
            try:
                self.vlan_binding = importutils.import_object(
                    self.conf.vlan_binding_driver, self.conf, self)
            except ImportError:
                LOG.error(_('Failed to import VLAN binding driver: %s'
                            % self.conf.vlan_binding_driver))
        if self.conf.l3_binding_driver:
            try:
                self.l3_binding = importutils.import_object(
                    self.conf.l3_binding_driver, self.conf, self)
            except ImportError:
                LOG.error(_('Failed to import L3 binding driver: %s'
                            % self.conf.l3_binding_driver))
        else:
            LOG.debug(_('No L3 binding driver configured.'
                        ' No L3 binding will be done.'))
            self.l3_binding = None

        if self.conf.f5_global_routed_mode:
            self.bigip_l2_manager = None
        else:
            self.fdb_connector = FDBConnectorML2(self.conf)
            self.bigip_l2_manager = BigipL2Manager(
                self.conf, self.vcmp_manager, self.fdb_connector,
                self.vlan_binding
            )

            # Direct means creating vlans, selfips, directly
            # rather than via iApp
            self.network_builder = NetworkBuilderDirect(
                self.conf, self, self.bigip_l2_manager, self.l3_binding
            )

        # Directly to the BIG-IP rather than through BIG-IQ.
        self.lbaas_builder_bigip_iapp = LBaaSBuilderBigipIApp(
            self.conf, self, self.bigip_l2_manager
        )
        # Object signifies creating vips, pools with iControl
        # rather than using iApp.
        self.lbaas_builder_bigip_objects = LBaaSBuilderBigipObjects(
            self.conf, self, self.bigip_l2_manager, self.l3_binding
        )
        try:
            self.lbaas_builder_bigiq_iapp = LBaaSBuilderBigiqIApp(
                self.conf, self
            )
        except NeutronException as exc:
            LOG.debug(_('Not using bigiq: %s' % exc.msg))

    def _init_bigip_hostnames(self):
        """ Validate and parse bigip credentials """
        if not self.conf.icontrol_hostname:
            raise InvalidConfigurationOption(
                opt_name='icontrol_hostname',
                opt_value='valid hostname or IP address'
            )
        if not self.conf.icontrol_username:
            raise InvalidConfigurationOption(
                opt_name='icontrol_username',
                opt_value='valid username'
            )
        if not self.conf.icontrol_password:
            raise InvalidConfigurationOption(
                opt_name='icontrol_password',
                opt_value='valid password'
            )

        self.hostnames = self.conf.icontrol_hostname.split(',')
        self.hostnames = [item.strip() for item in self.hostnames]
        self.hostnames = sorted(self.hostnames)

        # Setting an agent_id is the flag to the agent manager
        # that your plugin has initialized correctly. If you
        # don't set one, the agent manager will not register
        # with Neutron as a valid agent.
        if self.conf.environment_prefix:
            self.agent_id = str(
                uuid.uuid5(uuid.NAMESPACE_DNS,
                           self.conf.environment_prefix +
                           '.' + self.hostnames[0])
                )
        else:
            self.agent_id = str(
                uuid.uuid5(uuid.NAMESPACE_DNS, self.hostnames[0])
            )

        # initialize per host agent_configurations
        for hostname in self.hostnames:
            self.__bigips[hostname] = bigip = type('', (), {})()
            bigip.hostname = hostname
            bigip.status = 'creating'
            bigip.status_message = 'creating BIG-IP from iControl hostnames'
            self.agent_configurations[
                'icontrol_endpoints'][hostname] = {}
            self.agent_configurations[
                'icontrol_endpoints'][hostname]['failover_state'] = \
                'undiscovered'
            self.agent_configurations[
                'icontrol_endpoints'][hostname]['status'] = 'unknown'
            self.agent_configurations[
                'icontrol_endpoints'][hostname]['status_message'] = ''

    def _init_bigips(self):
        """ Connect to all BIG-IPs """
        if self.operational:
            LOG.debug('iControl driver reports connection is operational')
            return
        LOG.debug('initializing communications to BIG-IPs')
        try:
            if not self.conf.debug:
                sudslog = std_logging.getLogger('suds.client')
                sudslog.setLevel(std_logging.FATAL)
                requests_log = std_logging.getLogger(
                    "requests.packages.urllib3")
                requests_log.setLevel(std_logging.ERROR)
                requests_log.propagate = False

            else:
                requests_log = std_logging.getLogger(
                    "requests.packages.urllib3")
                requests_log.setLevel(std_logging.DEBUG)
                requests_log.propagate = True

            self.__last_connect_attempt = datetime.datetime.now()
            for hostname in self.hostnames:
                # connect to each BIG-IP and set it status
                bigip = self._open_bigip(hostname)
                if bigip.status == 'active':
                    # set the status down until we assure initialized
                    bigip.status = 'initializing'
                    bigip.status_message = 'initializing HA viability'
                    LOG.debug('initializing HA viability %s' % hostname)
                    device_group_name = None
                    if not self.ha_validated:
                        device_group_name = self._validate_ha(bigip)
                        LOG.debug('HA validated from %s with DSG %s' %
                                  (hostname, device_group_name))
                        self.ha_validated = True
                    if not self.tg_initialized:
                        self._init_traffic_groups(bigip)
                        LOG.debug('learned traffic groups from %s as %s' %
                                  (hostname, self.__traffic_groups))
                        self.tg_initialized = True
                    LOG.debug('initializing bigip %s' % hostname)
                    self._init_bigip(bigip, hostname, device_group_name)
                    LOG.debug('initializing agent configurations %s'
                              % hostname)
                    self._init_agent_config(bigip)
                    # Assure basic BIG-IP HA is operational
                    LOG.debug('validating HA state for %s' % hostname)
                    bigip.status = 'validating_HA'
                    bigip.status_message = 'validating the current HA state'
                    if self._validate_ha(bigip):
                        LOG.debug('setting status to active for %s' % hostname)
                        bigip.status = 'active'
                        bigip.status_message = 'BIG-IP ready for provisioning'
                        # self._post_init()
                    else:
                        LOG.debug('setting status to error for %s' % hostname)
                        bigip.status = 'error'
                        bigip.status_message = 'BIG-IP is not operational'
                        self._set_agent_status(False)
                else:
                    LOG.error('error opening BIG-IP %s - %s:%s'
                              % (hostname, bigip.status, bigip.status_message))
                    self._set_agent_status(False)
            # self.connected = True
        except Exception as exc:
            LOG.error('Invalid agent configuration: %s' % exc.message)
            raise
        self._set_agent_status(force_resync=True)

    def _init_errored_bigips(self):
        try:
            errored_bigips = self.get_errored_bigips_hostnames()
            if errored_bigips:
                LOG.debug('attempting to recover %s BIG-IPs' %
                          len(errored_bigips))
                for hostname in errored_bigips:
                    # try to connect and set status
                    bigip = self._open_bigip(hostname)
                    if bigip.status == 'active':
                        # set the status down until we assure initialized
                        bigip.status = 'initializing'
                        bigip.status_message = 'initializing HA viability'
                        LOG.debug('initializing HA viability %s' % hostname)
                        LOG.debug('proceeding to initialize %s' % hostname)
                        device_group_name = None
                        if not self.ha_validated:
                            device_group_name = self._validate_ha(bigip)
                            LOG.debug('HA validated from %s with DSG %s' %
                                      (hostname, device_group_name))
                            self.ha_validated = True
                        if not self.tg_initialized:
                            self._init_traffic_groups(bigip)
                            LOG.debug('known traffic groups initialized',
                                      ' from %s as %s' %
                                      (hostname, self.__traffic_groups))
                            self.tg_initialized = True
                        LOG.debug('initializing bigip %s' % hostname)
                        self._init_bigip(bigip, hostname, device_group_name)
                        LOG.debug('initializing agent configurations %s'
                                  % hostname)
                        self._init_agent_config(bigip)

                        # Assure basic BIG-IP HA is operational
                        LOG.debug('validating HA state for %s' % hostname)
                        bigip.status = 'validating_HA'
                        bigip.status_message = \
                            'validating the current HA state'
                        if self._validate_ha_operational(bigip):
                            LOG.debug('setting status to active for %s'
                                      % hostname)
                            bigip.status = 'active'
                            bigip.status_message = \
                                'BIG-IP ready for provisioning'
                            # self._post_init()
                            self._set_agent_status(True)
                        else:
                            LOG.debug('setting status to error for %s'
                                      % hostname)
                            bigip.status = 'error'
                            bigip.status_message = 'BIG-IP is not operational'
                            self._set_agent_status(False)
            else:
                LOG.debug('there are no BIG-IPs with error status')
        except Exception as exc:
            LOG.error('Invalid agent configuration: %s' % exc.message)
            raise

    def _open_bigip(self, hostname):
        """ Open bigip connection """
        LOG.info(_('Opening iControl connection to %s @ %s' %
                   (self.conf.icontrol_username, hostname)))
        # Open bigip connection """
        try:
            bigip = self.__bigips[hostname]
            if bigip.status not in ['creating', 'error']:
                LOG.debug('BIG-IP %s status invalid %s to open a connection'
                          % (hostname, bigip.status))
                return bigip
            bigip.status = 'connecting'
            bigip.status_message = 'requesting iControl endpoint'
            LOG.info('opening iControl connection to %s @ %s' %
                     (self.conf.icontrol_username, hostname))
            bigip = f5_bigip.BigIP(hostname, self.conf.icontrol_username,
                                   self.conf.icontrol_password,
                                   f5const.CONNECTION_TIMEOUT)
            bigip.status = 'active'
            bigip.hostname = hostname
            bigip.status_message = 'connected to BIG-IP'
            self.__bigips[hostname] = bigip
            return bigip

        except Exception as exc:
            LOG.exception('could not communicate with ' +
                          'iControl device: %s' % hostname)
            errbigip = type('', (), {})()
            errbigip.hostname = hostname
            errbigip.status = 'error'
            errbigip.status_message = str(exc)[:80]
            self.__bigips[hostname] = errbigip
            return errbigip

    def _init_bigip(self, bigip, hostname, check_group_name=None):
        # Prepare a bigip for usage
        try:
            bigip.system.set_folder('/Common')
            major_version, minor_version = _validate_bigip_version(bigip, hostname)

            device_group_name = None
            extramb = bigip.system.get_provision_extramb()
            if int(extramb) < f5const.MIN_EXTRA_MB:
                raise f5ex.ProvisioningExtraMBValidateFailed(
                    'Device %s BIG-IP not provisioned for '
                    'management LARGE.' % hostname)

            if self.conf.f5_ha_type == 'pair' and \
                            bigip.cluster.get_sync_status() == 'Standalone':
                raise f5ex.BigIPClusterInvalidHA(
                    'HA mode is pair and bigip %s in standalone mode'
                    % hostname)

            if self.conf.f5_ha_type == 'scalen' and \
                            bigip.cluster.get_sync_status() == 'Standalone':
                raise f5ex.BigIPClusterInvalidHA(
                    'HA mode is scalen and bigip %s in standalone mode'
                    % hostname)
            if self.conf.f5_ha_type != 'standalone':
                device_group_name = bigip.device.get_device_group()
                if not device_group_name:
                    raise f5ex.BigIPClusterInvalidHA(
                        'HA mode is %s and no sync failover '
                        'device group found for device %s.'
                        % (self.conf.f5_ha_type, hostname))
                if check_group_name and device_group_name != check_group_name:
                    raise f5ex.BigIPClusterInvalidHA(
                        'Invalid HA. Device %s is in device group'
                        ' %s but should be in %s.'
                        % (hostname, device_group_name, check_group_name))
                bigip.device_group_name = device_group_name

            for network in self.conf.common_network_ids.values():
                if not bigip.vlan.exists(network, folder='Common'):
                    raise f5ex.MissingNetwork(_(
                        'Common network %s on %s does not exist'
                        % (network, bigip.icontrol.hostname)))

            bigip.device_name = bigip.device.get_device_name()
            bigip.mac_addresses = bigip.interface.get_mac_addresses()
            LOG.debug("Initialized BIG-IP %s with MAC addresses %s" %
                      (bigip.device_name, ', '.join(bigip.mac_addresses)))
            bigip.device_interfaces = \
                bigip.interface.get_interface_macaddresses_dict()
            bigip.assured_networks = {}
            bigip.assured_tenant_snat_subnets = {}
            bigip.assured_gateway_subnets = []

            if self.conf.f5_ha_type != 'standalone':
                bigip.cluster.disable_auto_sync(device_group_name)

            # validate VTEP SelfIPs
            if not self.conf.f5_global_routed_mode:
                self.network_builder.initialize_tunneling(bigip)

            # Turn off tunnel syncing between BIG-IP
            # as our VTEPs properly use only local SelfIPs
            if bigip.system.get_tunnel_sync() == 'enable':
                bigip.system.set_tunnel_sync(enabled=False)

            LOG.debug('connected to iControl %s @ %s ver %s.%s'
                      % (self.conf.icontrol_username, hostname,
                         major_version, minor_version))

        except Exception as exc:
            bigip.status = 'error'
            bigip.status_message = str(exc)[:80]
            raise
        return bigip

    def _validate_ha(self, bigip):
        device_group_name = None
        if self.conf.f5_ha_type == 'standalone':
            if len(self.hostnames) != 1:
                bigip.status = 'error'
                bigip.status_message = \
                    'HA mode is standalone and %d hosts found.'\
                    % len(self.hostnames)
                raise f5ex.BigIPClusterInvalidHA(
                    'HA mode is standalone and %d hosts found.'
                    % len(self.hostnames))
            device_group_name = 'standalone'
        elif self.conf.f5_ha_type == 'pair':
            device_group_name = bigip.device.get_device_group()
            if len(self.hostnames) != 2:
                mgmt_addrs = []
                devices = bigip.cluster.devices(device_group_name)
                for device in devices:
                    mgmt_addrs.append(
                        bigip.device.get_mgmt_addr_by_device(device))
                self.hostnames = mgmt_addrs
            if len(self.hostnames) != 2:
                bigip.status = 'error'
                bigip.status_message = 'HA mode is pair and %d hosts found.' \
                    % len(self.hostnames)
                raise f5ex.BigIPClusterInvalidHA(
                    'HA mode is pair and %d hosts found.'
                    % len(self.hostnames))
        elif self.conf.f5_ha_type == 'scalen':
            device_group_name = bigip.device.get_device_group()
            if len(self.hostnames) < 2:
                mgmt_addrs = []
                devices = bigip.cluster.devices(device_group_name)
                for device in devices:
                    mgmt_addrs.append(
                        bigip.cluster.get_mgmt_addr_by_device(
                            bigip, device))
                self.hostnames = mgmt_addrs
            if len(self.hostnames) < 2:
                bigip.status = 'error'
                bigip.status_message = 'HA mode is scale and 1 hosts found.'
                raise f5ex.BigIPClusterInvalidHA(
                    'HA mode is pair and 1 hosts found.')
        return device_group_name

    def _validate_ha_operational(self, bigip):
        if self.conf.f5_ha_type == 'standalone':
            return True
        else:
            # how many active BIG-IPs are there?
            active_bigips = self.get_active_bigips()
            if active_bigips:
                sync_status = bigip.cluster.get_sync_status()
                if sync_status in ['Disconnected', 'Sync Failure']:
                    if len(active_bigips) > 1:
                        # the device should not be in the disconnected state
                        return False
                if len(active_bigips) > 1:
                    # it should be in the same sync-failover group
                    # as the rest of the active bigips
                    device_group_name = \
                        bigip.cluster.get_device_group()
                    for ab in active_bigips:
                        adgn = ab.cluster.get_device_group()
                        if not adgn == device_group_name:
                            return False
                return True
            else:
                return True

    def _init_agent_config(self, bigip):
        """ Init agent config """
        ic_host = {}
        ic_host['version'] = bigip.system.get_version()
        ic_host['device_name'] = bigip.device_name
        ic_host['platform'] = bigip.system.get_platform()
        ic_host['serial_number'] = bigip.system.get_serial_number()
        ic_host['status'] = bigip.status
        ic_host['status_message'] = bigip.status_message
        ic_host['failover_state'] = bigip.device.get_failover_state()

        if bigip.local_ip:
            ic_host['local_ip'] = bigip.local_ip
        else:
            ic_host['local_ip'] = 'VTEP disabled'
            self.agent_configurations['tunnel_types'] = 'None'
        self.agent_configurations['icontrol_endpoints'][bigip.hostname] = \
            ic_host

        if self.bigip_l2_manager:
            self.agent_configurations['bridge_mappings'] = \
                self.bigip_l2_manager.interface_mapping

    def get_failover_state(self, bigip):
        try:
            if hasattr(bigip, 'device'):
                fs = bigip.device.get_failover_state()
                bigip.failover_state = fs
                return bigip.failover_state
            else:
                return 'error'
        except Exception as exc:
            LOG.exception('Error getting %s failover state' % bigip.hostname)
            bigip.status = 'error'
            bigip.status_message = str(exc)[:80]
            self._set_agent_status(False)
            return 'error'

    def get_agent_configurations(self):
        # LOG.error('XXXX bigips:%s', self.__bigips)
        for hostname in self.__bigips:
            bigip = self.__bigips[hostname]
            if bigip.status == 'active':
                failover_state = self.get_failover_state(bigip)
                self.agent_configurations[
                    'icontrol_endpoints'][bigip.hostname][
                        'failover_state'] = failover_state
            else:
                self.agent_configurations[
                    'icontrol_endpoints'][bigip.hostname][
                        'failover_state'] = 'unknown'
            self.agent_configurations['icontrol_endpoints'][
                bigip.hostname]['status'] = bigip.status
            self.agent_configurations['icontrol_endpoints'][
                bigip.hostname]['status_message'] = bigip.status_message
            self.agent_configurations['operational'] = \
                self.operational
        LOG.debug('agent configurations are: %s' % self.agent_configurations)
        return dict(self.agent_configurations)

    def recover_errored_devices(self):
        # trigger a retry on errored BIG-IPs
        try:
            self._init_errored_bigips()
        except Exception as exc:
            LOG.error('Could not recover devices: %s' % exc.message)

    def backend_integrity(self):
        if self.operational:
            return True
        return False

    def generate_capacity_score(self, capacity_policy=None):
        """ Generate the capacity score of connected devices """
        if capacity_policy:
            highest_metric = 0.0
            highest_metric_name = None
            my_methods = dir(self)
            for metric in capacity_policy:
                func_name = 'get_' + metric
                if func_name in my_methods:
                    max_capacity = int(capacity_policy[metric])
                    metric_func = getattr(self, func_name)
                    global_stats = []
                    metric_value = 0
                    for host in self.__bigips:
                        hostbigip = self.__bigips[host]
                        global_stats = hostbigip.stat.get_global_statistics()
                        value = int(
                            metric_func(bigip=hostbigip,
                                        global_statistics=global_stats)
                        )
                        LOG.debug(_('calling capacity %s on %s returned: %s'
                                    % (func_name,
                                       hostbigip.icontrol.hostname,
                                       value)))
                        if value > metric_value:
                            metric_value = value
                    metric_capacity = float(metric_value) / float(max_capacity)
                    if metric_capacity > highest_metric:
                        highest_metric = metric_capacity
                        highest_metric_name = metric
                else:
                    LOG.warn(_('capacity policy has method '
                               '%s which is not implemented in this driver'
                               % metric))
            LOG.debug('capacity score: %s based on %s'
                      % (highest_metric, highest_metric_name))
            return highest_metric
        return 0

    def _set_agent_status(self, force_resync=False):
        for hostname in self.__bigips:
            bigip = self.__bigips[hostname]
            self.agent_configurations[
                'icontrol_endpoints'][bigip.hostname][
                    'status'] = bigip.status
            self.agent_configurations[
                'icontrol_endpoints'][bigip.hostname][
                    'status_message'] = bigip.status_message
        # Policy - if any BIG-IP are active we're operational
        if self.get_active_bigips():
            self.operational = True
        else:
            self.operational = False
        if self.agent_report_state:
            self.agent_report_state(force_resync=force_resync)

    def set_context(self, context):
        """ Context to keep for database access """
        self.context = context
        if self.fdb_connector:
            self.fdb_connector.set_context(context)

    def set_plugin_rpc(self, plugin_rpc):
        """ Provide Plugin RPC access """
        self.plugin_rpc = plugin_rpc

    def set_tunnel_rpc(self, tunnel_rpc):
        """ Provide FDB Connector with ML2 RPC access """
        if self.fdb_connector:
            self.fdb_connector.set_tunnel_rpc(tunnel_rpc)

    def set_l2pop_rpc(self, l2pop_rpc):
        """ Provide FDB Connector with ML2 RPC access """
        if self.fdb_connector:
            self.fdb_connector.set_l2pop_rpc(l2pop_rpc)

    def set_agent_report_state(self, report_state_callback):
        """Set Agent Report State"""
        self.agent_report_state = report_state_callback

    @serialized('exists')
    @is_operational
    def exists(self, service):
        """Check that service exists"""
        return self._service_exists(service)

    def flush_cache(self):
        """Remove cached objects so they can be created if necessary"""
        for bigip in self.get_all_bigips():
            bigip.assured_networks = []
            bigip.assured_tenant_snat_subnets = {}
            bigip.assured_gateway_subnets = []

    # pylint: disable=unused-argument
    @serialized('create_vip')
    @is_operational
    def create_vip(self, vip, service):
        """Create virtual server"""
        self._common_service_handler(service)

    @serialized('update_vip')
    @is_operational
    def update_vip(self, old_vip, vip, service):
        """Update virtual server"""
        self._common_service_handler(service)

    @serialized('delete_vip')
    @is_operational
    def delete_vip(self, vip, service):
        """Delete virtual server"""
        self._common_service_handler(service)

    @serialized('create_pool')
    @is_operational
    def create_pool(self, pool, service):
        """Create lb pool"""
        self._common_service_handler(service)

    @serialized('update_pool')
    @is_operational
    def update_pool(self, old_pool, pool, service):
        """Update lb pool"""
        self._common_service_handler(service)

    @serialized('delete_pool')
    @is_operational
    def delete_pool(self, pool, service):
        """Delete lb pool"""
        self._common_service_handler(service)

    @serialized('create_member')
    @is_operational
    def create_member(self, member, service):
        """Create pool member"""
        self._common_service_handler(service)

    @serialized('update_member')
    @is_operational
    def update_member(self, old_member, member, service):
        """Update pool member"""
        self._common_service_handler(service)

    @serialized('delete_member')
    @is_operational
    def delete_member(self, member, service):
        """Delete pool member"""
        self._common_service_handler(service)

    @serialized('create_pool_health_monitor')
    @is_operational
    def create_pool_health_monitor(self, health_monitor, pool, service):
        """Create pool health monitor"""
        self._common_service_handler(service)
        return True

    @serialized('update_health_monitor')
    @is_operational
    def update_health_monitor(self, old_health_monitor,
                              health_monitor, pool, service):
        """Update pool health monitor"""
        # The altered health monitor does not mark its
        # status as PENDING_UPDATE properly.  Force it.
        for i in range(len(service['pool']['health_monitors_status'])):
            if service['pool']['health_monitors_status'][i]['monitor_id'] == \
                    health_monitor['id']:
                service['pool']['health_monitors_status'][i]['status'] = \
                    plugin_const.PENDING_UPDATE
        self._common_service_handler(service)
        return True

    @serialized('delete_pool_health_monitor')
    @is_operational
    def delete_pool_health_monitor(self, health_monitor, pool, service):
        """Delete pool health monitor"""
        # Two behaviors of the plugin dictate our behavior here.
        # 1. When a plug-in deletes a monitor that is not being
        # used by a pool, it does not notify the drivers. Therefore,
        # we need to aggresively remove monitors that are not in use.
        # 2. When a plug-in deletes a monitor which is being
        # used by one or more pools, it calls delete_pool_health_monitor
        # against the driver that owns each pool, but it does not
        # set status to PENDING_DELETE in the health_monitors_status
        # list for the pool monitor. This may be a bug or perhaps this
        # is intended to be a synchronous process.
        #
        # In contrast, when a pool monitor association is deleted, the
        # PENDING DELETE status is set properly, so this code will
        # run unnecessarily in that case.
        for status in service['pool']['health_monitors_status']:
            if status['monitor_id'] == health_monitor['id']:
                # Signal to our own code that we should delete the
                # pool health monitor. The plugin should do this.
                status['status'] = plugin_const.PENDING_DELETE

        self._common_service_handler(service)
        return True
    # pylint: enable=unused-argument

    # these are the refactored methods
    def get_active_bigips(self):
        return self.get_all_bigips()

    def get_errored_bigips_hostnames(self):
        return_hostnames = []
        for host in list(self.__bigips):
            bigip = self.__bigips[host]
            if hasattr(bigip, 'status') and bigip.status == 'error':
                return_hostnames.append(host)
        return return_hostnames

    @is_operational
    def get_stats(self, service):
        """Get service stats"""
        # use pool stats because the pool_id is the
        # the service definition...
        stats = {}
        stats[lb_const.STATS_IN_BYTES] = 0
        stats[lb_const.STATS_OUT_BYTES] = 0
        stats[lb_const.STATS_ACTIVE_CONNECTIONS] = 0
        stats[lb_const.STATS_TOTAL_CONNECTIONS] = 0
        # add a members stats return dictionary
        members = {}
        for hostbigip in self.get_all_bigips():
            # It appears that stats are collected for pools in a pending delete
            # state which means that if those messages are queued (or delayed)
            # it can result in the process of a stats request after the pool
            # and tenant are long gone. Check if the tenant exists.
            if not service['pool'] or not hostbigip.system.folder_exists(
               bigip_interfaces.OBJ_PREFIX + service['pool']['tenant_id']):
                return None
            pool = service['pool']
            pool_stats = hostbigip.pool.get_statistics(
                name=pool['id'],
                folder=pool['tenant_id'],
                config_mode=self.conf.icontrol_config_mode)
            if 'STATISTIC_SERVER_SIDE_BYTES_IN' in pool_stats:
                stats[lb_const.STATS_IN_BYTES] += \
                    pool_stats['STATISTIC_SERVER_SIDE_BYTES_IN']
                stats[lb_const.STATS_OUT_BYTES] += \
                    pool_stats['STATISTIC_SERVER_SIDE_BYTES_OUT']
                stats[lb_const.STATS_ACTIVE_CONNECTIONS] += \
                    pool_stats['STATISTIC_SERVER_SIDE_CURRENT_CONNECTIONS']
                stats[lb_const.STATS_TOTAL_CONNECTIONS] += \
                    pool_stats['STATISTIC_SERVER_SIDE_TOTAL_CONNECTIONS']
                # are there members to update status
                if 'members' in service:
                    # only query BIG-IP pool members if they
                    # not in a state indicating provisioning or error
                    # provisioning the pool member
                    some_members_require_status_update = False
                    update_if_status = [plugin_const.ACTIVE,
                                        plugin_const.DOWN,
                                        plugin_const.INACTIVE]
                    if PLUGIN_CREATED_FLAG not in update_if_status:
                        update_if_status.append(PLUGIN_CREATED_FLAG)

                    for member in service['members']:
                        if member['status'] in update_if_status:
                            some_members_require_status_update = True
                    # are we have members who are in a
                    # state to update there status
                    if some_members_require_status_update:
                        # query pool members on each BIG-IP
                        monitor_states = \
                            hostbigip.pool.get_members_monitor_status(
                                name=pool['id'],
                                folder=pool['tenant_id'],
                                config_mode=self.conf.icontrol_config_mode
                            )
                        for member in service['members']:
                            if member['status'] in update_if_status:
                                # create the entry for this
                                # member in the return status
                                # dictionary set to ACTIVE
                                if not member['id'] in members:
                                    members[member['id']] = \
                                        {'status': plugin_const.INACTIVE}
                                # check if it down or up by monitor
                                # and update the status
                                for state in monitor_states:
                                    # matched the pool member
                                    # by address and port number
                                    if member['address'] == \
                                            strip_domain_address(
                                            state['addr']) and \
                                            int(member['protocol_port']) == \
                                            int(state['port']):
                                        # if the monitor says member is up
                                        if state['state'] == \
                                                'MONITOR_STATUS_UP' or \
                                           state['state'] == \
                                                'MONITOR_STATUS_UNCHECKED':
                                            # set ACTIVE as long as the
                                            # status was not set to 'DOWN'
                                            # on another BIG-IP
                                            if members[
                                                member['id']]['status'] != \
                                                    'DOWN':
                                                if member['admin_state_up']:
                                                    members[member['id']][
                                                        'status'] = \
                                                        plugin_const.ACTIVE
                                                else:
                                                    members[member['id']][
                                                        'status'] = \
                                                        plugin_const.INACTIVE
                                        else:
                                            members[member['id']]['status'] = \
                                                plugin_const.DOWN
        stats['members'] = members
        return stats

    @serialized('remove_orphans')
    def remove_orphans(self, all_pools):
        """ Remove out-of-date configuration on big-ips """
        existing_tenants = []
        existing_pools = []
        for pool in all_pools:
            existing_tenants.append(pool['tenant_id'])
            existing_pools.append(pool['pool_id'])
        for bigip in self.get_all_bigips():
            bigip.pool.purge_orphaned_pools(existing_pools)
        for bigip in self.get_all_bigips():
            bigip.system.purge_orphaned_folders_contents(existing_tenants)

        sudslog = std_logging.getLogger('suds.client')
        sudslog.setLevel(std_logging.FATAL)
        for bigip in self.get_all_bigips():
            bigip.system.force_root_folder()
        sudslog.setLevel(std_logging.ERROR)

        for bigip in self.get_all_bigips():
            bigip.system.purge_orphaned_folders(existing_tenants)

    def fdb_add(self, fdb):
        """ Add (L2toL3) forwarding database entries """
        self.remove_ips_from_fdb_update(fdb)
        for bigip in self.get_all_bigips():
            self.bigip_l2_manager.add_bigip_fdb(bigip, fdb)

    def fdb_remove(self, fdb):
        """ Remove (L2toL3) forwarding database entries """
        self.remove_ips_from_fdb_update(fdb)
        for bigip in self.get_all_bigips():
            self.bigip_l2_manager.remove_bigip_fdb(bigip, fdb)

    def fdb_update(self, fdb):
        """ Update (L2toL3) forwarding database entries """
        self.remove_ips_from_fdb_update(fdb)
        for bigip in self.get_all_bigips():
            self.bigip_l2_manager.update_bigip_fdb(bigip, fdb)

    # remove ips from fdb update so we do not try to
    # add static arps for them because we do not have
    # enough information to determine the route domain
    def remove_ips_from_fdb_update(self, fdb):
        for network_id in fdb:
            network = fdb[network_id]
            mac_ips_by_vtep = network['ports']
            for vtep in mac_ips_by_vtep:
                mac_ips = mac_ips_by_vtep[vtep]
                for mac_ip in mac_ips:
                    mac_ip[1] = None

    def tunnel_update(self, **kwargs):
        """ Tunnel Update from Neutron Core RPC """
        pass

    def tunnel_sync(self):
        """ Advertise all bigip tunnel endpoints """
        # Only sync when supported types are present
        if not [i for i in self.agent_configurations['tunnel_types']
                if i in ['gre', 'vxlan']]:
            return

        tunnel_ips = []
        for bigip in self.get_all_bigips():
            if bigip.local_ip:
                tunnel_ips.append(bigip.local_ip)
        if self.fdb_connector:
            self.fdb_connector.advertise_tunnel_ips(tunnel_ips)

    @serialized('sync')
    @is_operational
    def sync(self, service):
        """Sync service defintion to device"""
        # plugin_rpc may not be set when unit testing
        if self.plugin_rpc:
            # Get the latest service. It may have changed.
            service = self.plugin_rpc.get_service_by_pool_id(
                service['pool']['id'],
                self.conf.f5_global_routed_mode
            )
        if service['pool']:
            self._common_service_handler(service)
        else:
            LOG.debug("Attempted sync of deleted pool")

    @serialized('backup_configuration')
    @is_operational
    def backup_configuration(self):
        """ Save Configuration on Devices """
        for bigip in self.get_all_bigips():
            LOG.debug(_('_backup_configuration: saving device %s.'
                        % bigip.icontrol.hostname))
            bigip.cluster.save_config()

    def _service_exists(self, service):
        """ Returns whether the bigip has a pool for the service """
        if not service['pool']:
            return False
        folder_name = bigip_interfaces.OBJ_PREFIX + service['pool']['tenant_id']
        if self.lbaas_builder_bigiq_iapp:
            builder = self.lbaas_builder_bigiq_iapp
            readiness = builder.check_tenant_bigiq_readiness(service)
            use_bigiq = readiness['found_bigips']
        else:
            use_bigiq = False
        if use_bigiq:
            return self.lbaas_builder_bigiq_iapp.exists(service)
        else:
            bigips = self.get_config_bigips()
            for bigip in bigips:
                if not bigip.system.folder_exists(folder_name):
                    LOG.error("Folder %s does not exists on bigip: %s" %
                              (folder_name, bigip.hostname))
                    return False
                pl = bigip.pool.exists(name=service['pool']['id'],
                                       folder=service['pool']['tenant_id'],
                                       config_mode=self.conf.icontrol_config_mode)
                if not pl:
                    return False

                vs = bigip.virtual_server.get_virtual_servers_by_pool_name(
                    pool_name=bigip_interfaces.OBJ_PREFIX + service['pool']['id'],
                    folder=service['pool']['tenant_id']
                )
                if not vs:
                    return False

                for member in service['members']:
                    mb = bigip.pool.member_exists(
                        name=service['pool']['id'],
                        ip_address=member['address'],
                        port=member['protocol_port'],
                        folder=service['pool']['tenant_id']
                    )
                    if not mb:
                        return False
        return True

    def _common_service_handler(self, service):
        """ Assure that the service is configured on bigip(s) """
        start_time = time()

        if not service['pool']:
            LOG.error("_common_service_handler: Service pool is None")
            return

        # Here we look to see if the tenant has big-ips and
        # so we should use bigiq (if enabled) or fall back
        # to direct icontrol to the bigip(s).
        if self.lbaas_builder_bigiq_iapp:
            builder = self.lbaas_builder_bigiq_iapp
            readiness = builder.check_tenant_bigiq_readiness(service)
            use_bigiq = readiness['found_bigips']
        else:
            use_bigiq = False

        if not use_bigiq:
            self.tenant_manager.assure_tenant_created(service)
            LOG.debug("    _assure_tenant_created took %.5f secs" %
                      (time() - start_time))

        traffic_group = self._service_to_traffic_group(service)

        if not use_bigiq and self.network_builder:
            start_time = time()
            self.network_builder.prep_service_networking(
                service, traffic_group)
            if time() - start_time > .001:
                LOG.debug("    _prep_service_networking "
                          "took %.5f secs" % (time() - start_time))

        all_subnet_hints = {}
        if use_bigiq:
            self.lbaas_builder_bigiq_iapp.assure_service(
                service, traffic_group, all_subnet_hints)
        else:
            for bigip in self.get_config_bigips():
                # check_for_delete_subnets:
                #     keep track of which subnets we should check to delete
                #     for a deleted vip or member
                # do_not_delete_subnets:
                #     If we add an IP to a subnet we must not delete the subnet
                all_subnet_hints[bigip.device_name] = \
                    {'check_for_delete_subnets': {},
                     'do_not_delete_subnets': []}

            if self.conf.icontrol_config_mode == 'iapp':
                self.lbaas_builder_bigip_iapp.assure_service(
                    service, traffic_group, all_subnet_hints)
            else:
                self.lbaas_builder_bigip_objects.assure_service(
                    service, traffic_group, all_subnet_hints)

        if not use_bigiq and self.network_builder:
            start_time = time()
            try:
                self.network_builder.post_service_networking(
                    service, all_subnet_hints)
            except NeutronException as exc:
                LOG.error("post_service_networking exception: %s"
                          % str(exc.msg))
            except Exception as exc:
                LOG.error("post_service_networking exception: %s"
                          % str(exc.message))
            LOG.debug("    _post_service_networking took %.5f secs" %
                      (time() - start_time))

        if not use_bigiq:
            start_time = time()
            self.tenant_manager.assure_tenant_cleanup(
                service, all_subnet_hints)
            LOG.debug("    _assure_tenant_cleanup took %.5f secs" %
                      (time() - start_time))

        self.update_service_status(service)

        start_time = time()
        self.sync_if_clustered()
        LOG.debug("    final sync took %.5f secs" % (time() - start_time))

    def update_service_status(self, service):
        """ Update status of objects in OpenStack """

        # plugin_rpc may not be set when unit testing
        if not self.plugin_rpc:
            return
        self._update_members_status(service['members'])
        self._update_pool_status(service['pool'])
        self._update_pool_monitors_status(service)
        self._update_vip_status(service['vip'])

    def _update_members_status(self, members):
        """ Update member status in OpenStack """
        for member in members:
            if member['status'] == plugin_const.PENDING_CREATE:
                start_time = time()
                self.plugin_rpc.update_member_status(
                    member['id'],
                    status=PLUGIN_CREATED_FLAG,
                    status_description='member created')
                LOG.debug("            update_member_status"
                          " took %.5f secs" % (time() - start_time))
            elif member['status'] == plugin_const.PENDING_UPDATE:
                start_time = time()
                status = plugin_const.ACTIVE
                if 'admin_state_up' in member and \
                        not member['admin_state_up']:
                    status = plugin_const.INACTIVE
                self.plugin_rpc.update_member_status(
                    member['id'],
                    status=status,
                    status_description='member updated')
                LOG.debug("            update_member_status"
                          " took %.5f secs" % (time() - start_time))
            elif member['status'] == plugin_const.PENDING_DELETE:
                try:
                    self.plugin_rpc.member_destroyed(member['id'])
                except Exception as exc:
                    LOG.error(_("Plugin delete member %s error: %s"
                                % (member['id'], exc.message)))

    def _update_pool_status(self, pool):
        """ Update pool status in OpenStack """
        status = plugin_const.ACTIVE
        if 'admin_state_up' in pool and not pool['admin_state_up']:
            status = plugin_const.INACTIVE
        if pool['status'] == plugin_const.PENDING_UPDATE:
            self.plugin_rpc.update_pool_status(
                pool['id'],
                status=status,
                status_description='pool updated')
        elif pool['status'] == plugin_const.PENDING_CREATE:
            self.plugin_rpc.update_pool_status(
                pool['id'],
                status=status,
                status_description='pool created')
        elif pool['status'] == plugin_const.PENDING_DELETE:
            try:
                self.plugin_rpc.pool_destroyed(pool['id'])
            except Exception as exc:
                LOG.error(_("Plugin destroy pool %s error: %s"
                            % (pool['id'], exc.message)))

    def _update_pool_monitors_status(self, service):
        """ Update pool monitor status in OpenStack """
        monitors_destroyed = []
        monitors_updated = []
        pool = service['pool']

        LOG.debug("update_pool_monitors_status: service: %s" % service)
        health_monitors_status = {}
        for monitor in pool['health_monitors_status']:
            health_monitors_status[monitor['monitor_id']] = \
                monitor['status']

        LOG.debug("update_pool_monitors_status: health_monitor_status: %s"
                  % health_monitors_status)
        for monitor in service['health_monitors']:
            if monitor['id'] in health_monitors_status:
                if health_monitors_status[monitor['id']] == \
                        plugin_const.PENDING_DELETE:
                    monitors_destroyed.append(
                        {'health_monitor_id': monitor['id'],
                         'pool_id': pool['id']})
                elif health_monitors_status[monitor['id']] == \
                        plugin_const.PENDING_UPDATE or \
                        health_monitors_status[monitor['id']] == \
                        plugin_const.PENDING_CREATE:
                    monitors_updated.append(
                        {'pool_id': pool['id'],
                         'health_monitor_id': monitor['id'],
                         'status': plugin_const.ACTIVE,
                         'status_description': 'monitor active'})

        LOG.debug("Monitors to destroy: %s" % monitors_destroyed)
        for monitor_destroyed in monitors_destroyed:
            LOG.debug("Monitor destroying: %s" % monitor_destroyed)
            self.plugin_rpc.health_monitor_destroyed(
                **monitor_destroyed)
        for monitor_updated in monitors_updated:
            try:
                self.plugin_rpc.update_health_monitor_status(
                    **monitor_updated)
            except Exception as exc:
                if 'PENDING_DELETE' in str(exc):
                    LOG.debug("Attempted to update monitor being deleted!")
                else:
                    LOG.debug(str(exc))
                    raise

    def _update_vip_status(self, vip):
        """ Update vip status in OpenStack """
        status = plugin_const.ACTIVE
        if 'admin_state_up' in vip and not vip['admin_state_up']:
            status = plugin_const.INACTIVE
        if 'id' not in vip:
            return
        if vip['status'] == plugin_const.PENDING_CREATE:
            self.plugin_rpc.update_vip_status(
                vip['id'],
                status=status,
                status_description=None)
        elif vip['status'] == plugin_const.PENDING_UPDATE:
            self.plugin_rpc.update_vip_status(
                vip['id'],
                status=status,
                status_description=None)
        elif vip['status'] == plugin_const.PENDING_DELETE:
            try:
                self.plugin_rpc.vip_destroyed(vip['id'])
            except Exception as exc:
                LOG.error(_("Plugin delete vip %s error: %s"
                            % (vip['id'], exc.message)))

    def _service_to_traffic_group(self, service):
        """ Hash service tenant id to index of traffic group """
        return self.tenant_to_traffic_group(service['pool']['tenant_id'])

    def tenant_to_traffic_group(self, tenant_id):
        """ Hash tenant id to index of traffic group """
        hexhash = hashlib.md5(tenant_id).hexdigest()
        tg_index = int(hexhash, 16) % len(self.__traffic_groups)
        return self.__traffic_groups[tg_index]

    def get_bigip(self):
        """ Get one consistent big-ip """
        hostnames = sorted(self.__bigips)
        for i in range(len(hostnames)):
            try:
                bigip = self.__bigips[hostnames[i]]
                return bigip
            except urllib2.URLError:
                pass
        raise urllib2.URLError('cannot communicate to any bigips')

    def get_bigip_hosts(self):
        """ Get all big-ips hostnames under management """
        return self.__bigips

    def get_all_bigips(self):
        """ Get all big-ips under management """
        return_bigips = []
        for host in list(self.__bigips):
            if hasattr(self.__bigips[host], 'status') and \
                            self.__bigips[host].status == 'active':
                return_bigips.append(self.__bigips[host])
        return return_bigips

    def get_config_bigips(self):
        """ Return a list of big-ips that need to be configured.
            In replication sync mode, we configure all big-ips
            individually. In autosync mode we only use one big-ip
            and then sync the configuration to the other big-ips.
        """
        if self.conf.f5_sync_mode == 'replication':
            return self.get_all_bigips()
        else:
            return [self.get_bigip()]

    def get_inbound_throughput(self, bigip, global_statistics=None):
        if bigip:
            return bigip.stat.get_inbound_throughput(
                       global_stats=global_statistics)

    def get_outbound_throughput(self, bigip, global_statistics=None):
        if bigip:
            return bigip.stat.get_outbound_throughput(
                       global_stats=global_statistics)

    def get_throughput(self, bigip=None, global_statistics=None):
        if bigip:
            return bigip.stat.get_throughput(global_stats=global_statistics)

    def get_active_connections(self, bigip=None, global_statistics=None):
        if bigip:
            return bigip.stat.get_active_connection_count(
                   global_stats=global_statistics)

    def get_ssltps(self, bigip=None, global_statistics=None):
        if bigip:
            return bigip.stat.get_active_SSL_TPS(
                       global_stats=global_statistics)

    def get_node_count(self, bigip=None, global_statistics=None):
        if bigip:
            return bigip.pool.get_all_node_count()

    def get_clientssl_profile_count(self, bigip=None, global_statistics=None):
        if bigip:
            return len(bigip.ssl.all_client_profile_names())

    def get_tenant_count(self, bigip=None, global_statistics=None):
        if bigip:
            folders = bigip.system.get_folders()
            folders.remove('/')
            folders.remove('Common')
            return len(folders)

    def get_tunnel_count(self, bigip=None, global_statistics=None):
        if bigip:
            vxlan_tunnels = bigip.vxlan.get_tunnels(folder='/')
            gre_tunnels = bigip.l2gre.get_tunnels(folder='/')
            return len(vxlan_tunnels) + len(gre_tunnels)

    def get_vlan_count(self, bigip=None, global_statistics=None):
        if bigip:
            return len(bigip.vlan.get_vlans(folder='/'))

    def get_route_domain_count(self, bigip=None, global_statistics=None):
        if bigip:
            domain_ids = bigip.route.get_domain_ids(folder='/')
            domain_ids.remove(0)
            return len(domain_ids)

    def _init_traffic_groups(self, bigip):
        """ Count vips and gws on traffic groups """
        self.__traffic_groups = bigip.cluster.get_traffic_groups()
        if 'traffic-group-local-only' in self.__traffic_groups:
            self.__traffic_groups.remove('traffic-group-local-only')
        self.__traffic_groups.sort()

    def sync_if_clustered(self):
        """ sync device group if not in replication mode """
        if self.conf.f5_ha_type == 'standalone' or \
                self.conf.f5_sync_mode == 'replication' or \
                len(self.get_all_bigips()) < 2:
            return
        bigip = self.get_bigip()
        self._sync_with_retries(bigip)

    def _sync_with_retries(self, bigip, force_now=False,
                           attempts=4, retry_delay=130):
        """ sync device group """
        for attempt in range(1, attempts + 1):
            LOG.debug('Syncing Cluster... attempt %d of %d'
                      % (attempt, attempts))
            try:
                if attempt != 1:
                    force_now = False
                bigip.cluster.sync(bigip.device_group_name,
                                   force_now=force_now)
                LOG.debug('Cluster synced.')
                return
            except Exception as exc:
                LOG.error('ERROR: Cluster sync failed: %s' % exc)
                if attempt == attempts:
                    raise
                LOG.error('Wait another %d seconds for devices '
                          'to recover from failed sync.' % retry_delay)
                greenthread.sleep(retry_delay)


def _validate_bigip_version(bigip, hostname):
    """ Ensure the BIG-IP has sufficient version """
    major_version = bigip.system.get_major_version()
    if major_version < f5const.MIN_TMOS_MAJOR_VERSION:
        raise f5ex.MajorVersionValidateFailed(
            'Device %s must be at least TMOS %s.%s'
            % (hostname, f5const.MIN_TMOS_MAJOR_VERSION,
               f5const.MIN_TMOS_MINOR_VERSION))
    minor_version = bigip.system.get_minor_version()
    if minor_version < f5const.MIN_TMOS_MINOR_VERSION:
        raise f5ex.MinorVersionValidateFailed(
            'Device %s must be at least TMOS %s.%s'
            % (hostname, f5const.MIN_TMOS_MAJOR_VERSION,
               f5const.MIN_TMOS_MINOR_VERSION))
    return major_version, minor_version
