import hashlib
import inspect
import re
from typing import Any, Callable, Dict, List, Mapping, NamedTuple, Optional, Sequence, Set, cast

from dagster_fivetran.resources import DEFAULT_POLL_INTERVAL, FivetranResource
from dagster_fivetran.utils import (
    generate_materializations,
    get_fivetran_connector_url,
    metadata_for_table,
)

from dagster import AssetKey, AssetOut, AssetsDefinition, Output
from dagster import _check as check
from dagster import multi_asset
from dagster._annotations import experimental
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.definitions.events import CoercibleToAssetKeyPrefix
from dagster._core.definitions.load_assets_from_modules import with_group
from dagster._core.definitions.metadata import MetadataUserInput
from dagster._core.definitions.resource_definition import ResourceDefinition
from dagster._core.execution.context.init import build_init_resource_context
from dagster._core.execution.with_resources import with_resources


@experimental
def build_fivetran_assets(
    connector_id: str,
    destination_tables: Sequence[str],
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    poll_timeout: Optional[float] = None,
    io_manager_key: Optional[str] = None,
    asset_key_prefix: Optional[Sequence[str]] = None,
    metadata_by_table_name: Optional[Mapping[str, MetadataUserInput]] = None,
) -> Sequence[AssetsDefinition]:

    """
    Build a set of assets for a given Fivetran connector.

    Returns an AssetsDefinition which connects the specified ``asset_keys`` to the computation that
    will update them. Internally, executes a Fivetran sync for a given ``connector_id``, and
    polls until that sync completes, raising an error if it is unsuccessful. Requires the use of the
    :py:class:`~dagster_fivetran.fivetran_resource`, which allows it to communicate with the
    Fivetran API.

    Args:
        connector_id (str): The Fivetran Connector ID that this op will sync. You can retrieve this
            value from the "Setup" tab of a given connector in the Fivetran UI.
        destination_tables (List[str]): `schema_name.table_name` for each table that you want to be
            represented in the Dagster asset graph for this connection.
        poll_interval (float): The time (in seconds) that will be waited between successive polls.
        poll_timeout (Optional[float]): The maximum time that will waited before this operation is
            timed out. By default, this will never time out.
        io_manager_key (Optional[str]): The io_manager to be used to handle each of these assets.
        asset_key_prefix (Optional[List[str]]): A prefix for the asset keys inside this asset.
            If left blank, assets will have a key of `AssetKey([schema_name, table_name])`.

    Examples:

    .. code-block:: python

        from dagster import AssetKey, repository, with_resources

        from dagster_fivetran import fivetran_resource
        from dagster_fivetran.assets import build_fivetran_assets

        my_fivetran_resource = fivetran_resource.configured(
            {
                "api_key": {"env": "FIVETRAN_API_KEY"},
                "api_secret": {"env": "FIVETRAN_API_SECRET"},
            }
        )

        fivetran_assets = build_fivetran_assets(
            connector_id="foobar",
            table_names=["schema1.table1", "schema2.table2"],
        ])

        @repository
        def repo():
            return with_resources(
                fivetran_assets,
                resource_defs={"fivetran": my_fivetran_resource},
            )

    """

    asset_key_prefix = check.opt_sequence_param(asset_key_prefix, "asset_key_prefix", of_type=str)

    tracked_asset_keys = {
        table: AssetKey([*asset_key_prefix, *table.split(".")]) for table in destination_tables
    }

    metadata_by_table_name = check.opt_mapping_param(
        metadata_by_table_name, "metadata_by_table_name", key_type=str
    )

    @multi_asset(
        name=f"fivetran_sync_{connector_id}",
        outs={
            "_".join(key.path): AssetOut(
                io_manager_key=io_manager_key, key=key, metadata=metadata_by_table_name.get(table)
            )
            for table, key in tracked_asset_keys.items()
        },
        required_resource_keys={"fivetran"},
        compute_kind="fivetran",
    )
    def _assets(context):
        fivetran_output = context.resources.fivetran.sync_and_poll(
            connector_id=connector_id,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )
        for materialization in generate_materializations(
            fivetran_output, asset_key_prefix=asset_key_prefix
        ):
            # scan through all tables actually created, if it was expected then emit an Output.
            # otherwise, emit a runtime AssetMaterialization
            if materialization.asset_key in tracked_asset_keys.values():
                yield Output(
                    value=None,
                    output_name="_".join(materialization.asset_key.path),
                    metadata={
                        entry.label: entry.entry_data for entry in materialization.metadata_entries
                    },
                )
            else:
                yield materialization

    return [_assets]


class FivetranConnectionMetadata(
    NamedTuple(
        "_FivetranConnectionMetadata",
        [
            ("name", str),
            ("connector_id", str),
            ("connector_url", str),
            ("schemas", Mapping[str, Any]),
        ],
    )
):
    def build_asset_defn_metadata(
        self,
        key_prefix: Sequence[str],
        group_name: Optional[str],
    ) -> AssetsDefinitionCacheableData:

        schema_table_meta: Dict[str, MetadataUserInput] = {}
        if "schemas" in self.schemas:
            schemas_inner = cast(Dict[str, Any], self.schemas["schemas"])
            for schema in schemas_inner.values():
                schema_name = schema["name_in_destination"]
                schema_tables = cast(Dict[str, Dict[str, Any]], schema["tables"])
                for table in schema_tables.values():
                    if table["enabled"]:
                        table_name = table["name_in_destination"]
                        schema_table_meta[f"{schema_name}.{table_name}"] = metadata_for_table(
                            table, self.connector_url
                        )
        else:
            schema_table_meta[self.name] = {}

        outputs = {table: AssetKey([*key_prefix, table]) for table in schema_table_meta.keys()}

        internal_deps: Dict[str, Set[AssetKey]] = {}

        return AssetsDefinitionCacheableData(
            keys_by_input_name={},
            keys_by_output_name=outputs,
            internal_asset_deps=internal_deps,
            group_name=group_name,
            key_prefix=key_prefix,
            can_subset=False,
            metadata_by_output_name=schema_table_meta,
            extra_metadata={
                "connector_id": self.connector_id,
            },
        )


def _build_fivetran_assets_from_metadata(
    assets_defn_meta: AssetsDefinitionCacheableData,
) -> AssetsDefinition:
    connector_id = check.not_none(assets_defn_meta.extra_metadata)["connector_id"]

    return with_group(
        build_fivetran_assets(
            connector_id=connector_id,
            destination_tables=list(
                assets_defn_meta.keys_by_output_name.keys()
                if assets_defn_meta.keys_by_output_name
                else []
            ),
            asset_key_prefix=list(assets_defn_meta.key_prefix or []),
            metadata_by_table_name=cast(
                Dict[str, MetadataUserInput], assets_defn_meta.metadata_by_output_name
            ),
        ),
        assets_defn_meta.group_name,
    )[0]


class FivetranInstanceCacheableAssetsDefinition(CacheableAssetsDefinition):
    def __init__(
        self,
        fivetran_resource_def: ResourceDefinition,
        key_prefix: Sequence[str],
        connector_to_group_fn: Optional[Callable[[str], Optional[str]]],
        connector_filter: Optional[Callable[[FivetranConnectionMetadata], bool]],
    ):

        self._fivetran_resource_def = fivetran_resource_def
        self._fivetran_instance: FivetranResource = fivetran_resource_def(
            build_init_resource_context()
        )

        self._key_prefix = key_prefix
        self._connector_to_group_fn = connector_to_group_fn
        self._connection_filter = connector_filter

        contents = hashlib.sha1()
        contents.update(",".join(key_prefix).encode("utf-8"))
        if connector_filter:
            contents.update(inspect.getsource(connector_filter).encode("utf-8"))

        super().__init__(unique_id=f"fivetran-{contents.hexdigest()}")

    def _get_connectors(self) -> Sequence[FivetranConnectionMetadata]:
        output_connectors: List[FivetranConnectionMetadata] = []

        groups = self._fivetran_instance.make_request("GET", "groups")["items"]

        for group in groups:
            group_id = group["id"]

            connectors = self._fivetran_instance.make_request(
                "GET", f"groups/{group_id}/connectors"
            )["items"]
            for connector in connectors:
                connector_id = connector["id"]

                connector_name = connector["schema"]
                connector_url = get_fivetran_connector_url(connector)

                schemas = self._fivetran_instance.make_request(
                    "GET", f"connectors/{connector_id}/schemas"
                )

                output_connectors.append(
                    FivetranConnectionMetadata(
                        name=connector_name,
                        connector_id=connector_id,
                        connector_url=connector_url,
                        schemas=schemas,
                    )
                )

        return output_connectors

    def compute_cacheable_data(self) -> Sequence[AssetsDefinitionCacheableData]:

        asset_defn_data: List[AssetsDefinitionCacheableData] = []
        for connector in self._get_connectors():

            if not self._connection_filter or self._connection_filter(connector):

                asset_defn_data.append(
                    connector.build_asset_defn_metadata(
                        self._key_prefix,
                        self._connector_to_group_fn(connector.name)
                        if self._connector_to_group_fn
                        else None,
                    )
                )

        return asset_defn_data

    def build_definitions(
        self, data: Sequence[AssetsDefinitionCacheableData]
    ) -> Sequence[AssetsDefinition]:
        return with_resources(
            [_build_fivetran_assets_from_metadata(meta) for meta in data],
            {"fivetran": self._fivetran_resource_def},
        )


def _clean_name(name: str) -> str:
    """
    Cleans an input to be a valid Dagster asset name.
    """
    return re.sub(r"[^a-z0-9]+", "_", name.lower())


@experimental
def load_assets_from_fivetran_instance(
    fivetran: ResourceDefinition,
    key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
    connector_to_group_fn: Optional[Callable[[str], Optional[str]]] = _clean_name,
    connector_filter: Optional[Callable[[FivetranConnectionMetadata], bool]] = None,
) -> CacheableAssetsDefinition:
    """
    Loads Fivetran connector assets from a configured FivetranResource instance. This fetches information
    about defined connectors at initialization time, and will error on workspace load if the Fivetran
    instance is not reachable.

    Args:
        fivetran (ResourceDefinition): A FivetranResource configured with the appropriate connection
            details.
        key_prefix (Optional[CoercibleToAssetKeyPrefix]): A prefix for the asset keys created.
        connector_to_group_fn (Optional[Callable[[str], Optional[str]]]): Function which returns an asset
            group name for a given Fivetran connector name. If None, no groups will be created. Defaults
            to a basic sanitization function.
        connector_filter (Optional[Callable[[FivetranConnectorMetadata], bool]]): Optional function which takes
            in connector metadata and returns False if the connector should be excluded from the output assets.

    **Examples:**

    Loading all Fivetran connectors as assets:

    .. code-block:: python

        from dagster_fivetran import fivetran_resource, load_assets_from_fivetran_instance

        fivetran_instance = fivetran_resource.configured(
            {
                "api_key": "some_key",
                "api_secret": "some_secret",
            }
        )
        fivetran_assets = load_assets_from_fivetran_instance(fivetran_instance)

    Filtering the set of loaded connectors:

    .. code-block:: python

        from dagster_fivetran import fivetran_resource, load_assets_from_fivetran_instance

        fivetran_instance = fivetran_resource.configured(
            {
                "api_key": "some_key",
                "api_secret": "some_secret",
            }
        )
        fivetran_assets = load_assets_from_fivetran_instance(
            fivetran_instance,
            connection_filter=lambda meta: "snowflake" in meta.name,
        )
    """

    if isinstance(key_prefix, str):
        key_prefix = [key_prefix]
    key_prefix = check.list_param(key_prefix or [], "key_prefix", of_type=str)

    return FivetranInstanceCacheableAssetsDefinition(
        fivetran,
        key_prefix,
        connector_to_group_fn,
        connector_filter,
    )
