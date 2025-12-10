from dagster import ConfigurableResource
from ..utils.airbyte import get_workspace_id
from functools import lru_cache


class AirbyteWorkspaceConfigs(ConfigurableResource):
    @lru_cache()
    def workspace_id(self):
        return get_workspace_id()
