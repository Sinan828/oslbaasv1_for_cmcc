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
from collections import defaultdict
import random
import json

import f5.oslbaasv1driver.drivers.constants as lbaasv1const

try:
    from neutron.services.loadbalancer import agent_scheduler
    from neutron.openstack.common import log as logging
    from neutron.extensions.lbaas_agentscheduler import NoActiveLbaasAgent
except ImportError:
    # Kilo
    from neutron_lbaas.services.loadbalancer import agent_scheduler
    from oslo_log import log as logging
    from neutron_lbaas.extensions.lbaas_agentscheduler \
        import NoActiveLbaasAgent, NoEligibleLbaasAgent

LOG = logging.getLogger(__name__)


class TenantScheduler(agent_scheduler.ChanceScheduler):
    """Allocate a loadbalancer agent for a pool based on tenant_id.
       or else make a random choice.
    """
    def __init__(self):
        super(TenantScheduler, self).__init__()

    def get_lbaas_agent_hosting_pool(self, plugin, context, pool_id, env=None):
        """Return the agent that is hosting the loadbalancer."""
        LOG.debug('Getting agent for pool %s with env %s' %
                  (pool_id, env))

        with context.session.begin(subtransactions=True):
            # returns {'agent': agent_dict}
            lbaas_agent = plugin.get_lbaas_agent_hosting_pool(context,
                                                              pool_id)
            # if the agent bound to this loadbalancer is alive, return it
            if lbaas_agent is not None:
                if (not lbaas_agent['agent']['alive'] or
                        not lbaas_agent['agent']['admin_state_up']) and \
                        env is not None:
                    # The agent bound to this loadbalancer is not live
                    # or is not active. Find another agent in the same
                    # environment and environment group if possible
                    ac = self.deserialize_agent_configurations(
                        lbaas_agent['agent']['configurations']
                    )
                    # get a environment group number for the bound agent
                    if 'environment_group_number' in ac:
                        gn = ac['environment_group_number']
                    else:
                        gn = 1

                    reassigned_agent = self.rebind_pools(
                        plugin, context, env, gn, lbaas_agent['agent'])
                    if reassigned_agent:
                        lbaas_agent = {'agent': reassigned_agent}

            return lbaas_agent

    def rebind_pools(
            self, plugin, context, env, group, current_agent):
        env_agents = self.get_agents_in_env(plugin, context, env,
                                            group=group, active=True)
        if env_agents:
            reassigned_agent = env_agents[0]
            bindings = \
                context.session.query(
                    agent_scheduler.PoolLoadbalancerAgentBinding).filter_by(
                        agent_id=current_agent['id']).all()
            for binding in bindings:
                binding.agent_id = reassigned_agent['id']
                context.session.add(binding)
            LOG.debug("%s Pools bound to agent %s now bound to %s" %
                      (len(bindings),
                       current_agent['id'],
                       reassigned_agent['id']))
            return reassigned_agent
        else:
            return None

    def get_dead_agents_in_env(
            self, plugin, context, env, group=None):
        return_agents = []
        all_agents = self.get_agents_in_env(plugin,
                                            context,
                                            env,
                                            group,
                                            active=None)

        for agent in all_agents:
            if not plugin.is_eligible_agent(active=True, agent=agent):
                if not agent['admin_state_up']:
                    return_agents.append(agent)
        return return_agents

    def scrub_dead_agents(self, plugin, context, env, group=None):
        dead_agents = self.get_dead_agents_in_env(plugin, context, env, group)
        for agent in dead_agents:
            self.rebind_pools(plugin, context, env, group, agent)

    def get_active_agents_in_env(self, plugin, context, env, group=None):
        with context.session.begin(subtransactions=True):
            candidates = plugin.get_lbaas_agents(context, active=True)
            return_agents = []
            if candidates:
                for candidate in candidates:
                    ac = self.deserialize_agent_configurations(
                            candidate['configurations']
                    )
                    if 'environment_prefix' in ac:
                        if ac['environment_prefix'] == env:
                            if group:
                                if 'environment_group_number' in ac and \
                                  ac['environment_group_number'] == group:
                                    return_agents.append(candidate)
                            else:
                                return_agents.append(candidate)
            return return_agents

    def get_agents_in_env(self, plugin, context, env, group=None, active=None):
        """Get an active agents in the specified environment."""
        return_agents = []

        with context.session.begin(subtransactions=True):
            candidates = []
            try:
                candidates = plugin.get_lbaas_agents(context, active=active)
            except Exception as ex:
                LOG.error("Exception retrieving agent candidates for "
                          "scheduling: {}".format(ex))

            for candidate in candidates:
                ac = self.deserialize_agent_configurations(
                    candidate['configurations'])
                if 'environment_prefix' in ac:
                    if ac['environment_prefix'] == env:
                        if group:
                            if ('environment_group_number' in ac and
                                    ac['environment_group_number'] == group):
                                return_agents.append(candidate)
                        else:
                            return_agents.append(candidate)

        return return_agents

    def get_capacity(self, configurations):
        if 'environment_capacity_score' in configurations:
            return configurations['environment_capacity_score']
        else:
            return 0.0

    def deserialize_agent_configurations(self, agent_conf):
        """Return a dictionary for the agent configuration."""
        if not isinstance(agent_conf, dict):
            try:
                agent_conf = json.loads(agent_conf)
            except ValueError as ve:
                LOG.error("Can't decode JSON %s : %s"
                          % (agent_conf, ve.message))
                return {}
        return agent_conf

    def schedule(self, plugin, context, pool, env=None):
        """Schedule the pool to an active loadbalancer agent if there
        is no enabled agent hosting it.
        """
        with context.session.begin(subtransactions=True):
            pool = plugin.get_pool(context, pool.id)
            # If the loadbalancer is hosted on an active agent
            # already, return that agent or one in its env
            lbaas_agent = self.get_lbaas_agent_hosting_pool(
                plugin,
                context,
                pool.id,
                env
            )

            if lbaas_agent:
                lbaas_agent = lbaas_agent['agent']
                LOG.debug(' Assigning task to agent %s.'
                          % (lbaas_agent['id']))
                return lbaas_agent

            # There is no existing loadbalancer agent binding.
            # Find all active agent candidates in this env.
            # We use environment_prefix to find F5 agents
            # rather then map to the agent binary name.
            candidates = self.get_agents_in_env(
                plugin,
                context,
                env,
                active=True
            )

            LOG.debug("candidate agents: %s", candidates)
            if len(candidates) == 0:
                LOG.error('No f5 lbaas agents are active for env %s' % env)
                raise NoActiveLbaasAgent(
                    pool_id=pool.id)

            # We have active candidates to choose from.
            # Qualify them by tenant affinity and then capacity.
            chosen_agent = None
            agents_by_group = defaultdict(list)
            capacity_by_group = {}

            for candidate in candidates:
                # Organize agents by their environment group
                # and collect each group's max capacity.
                ac = self.deserialize_agent_configurations(
                    candidate['configurations']
                )
                gn = 1
                if 'environment_group_number' in ac:
                    gn = ac['environment_group_number']
                agents_by_group[gn].append(candidate)

                # populate each group's capacity
                group_capacity = self.get_capacity(ac)
                if gn not in capacity_by_group:
                    capacity_by_group[gn] = group_capacity
                else:
                    if group_capacity > capacity_by_group[gn]:
                        capacity_by_group[gn] = group_capacity

                # Do we already have this tenant assigned to this
                # agent candidate? If we do and it has capacity
                # then assign this loadbalancer to this agent.
                assigned_lbs = plugin.list_pools_on_lbaas_agent(
                    context, candidate['id'])
                for assigned_lb in assigned_lbs:
                    if pool.tenant_id == assigned_lb.tenant_id:
                        chosen_agent = candidate
                        break

                if chosen_agent:
                    # Does the agent which had tenants assigned
                    # to it still have capacity?
                    if group_capacity >= 1.0:
                        chosen_agent = None
                    else:
                        break

            # If we don't have an agent with capacity associated
            # with our tenant_id, let's pick an agent based on
            # the group with the lowest capacity score.
            if not chosen_agent:
                # lets get an agent from the group with the
                # lowest capacity score
                lowest_utilization = 1.0
                selected_group = 1
                for group, capacity in capacity_by_group.items():
                    if capacity < lowest_utilization:
                        lowest_utilization = capacity
                        selected_group = group

                LOG.debug('%s group %s scheduled with capacity %s'
                          % (env, selected_group, lowest_utilization))
                if lowest_utilization < 1.0:
                    # Choose a agent in the env group for this
                    # tenant at random.
                    chosen_agent = random.choice(
                        agents_by_group[selected_group]
                    )

            # If there are no agents with available capacity, raise exception
            if not chosen_agent:
                LOG.warn('No capacity left on any agents in env: %s' % env)
                LOG.warn('Group capacity in environment %s were %s.'
                         % (env, capacity_by_group))
                raise NoEligibleLbaasAgent(
                    pool_id=pool.id)

            binding = agent_scheduler.PoolLoadbalancerAgentBinding()
            binding.agent = chosen_agent
            binding.pool_id = pool.id
            context.session.add(binding)

            LOG.debug(('Pool %(pool_id)s is scheduled to '
                       'lbaas agent %(agent_id)s'),
                      {'pool_id': pool.id,
                       'agent_id': chosen_agent['id']})

            return chosen_agent
