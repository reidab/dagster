from typing import List

import graphene
from dagster_graphql.implementation.asset_subscription import AssetLogsEventsSubscribe
from dagster_graphql.implementation.events import from_dagster_event_record, from_event_record
from dagster_graphql.implementation.fetch_assets import asset_node_iter, get_asset_nodes
from dagster_graphql.implementation.fetch_solids import get_solid, get_solids
from dagster_graphql.implementation.loader import RepositoryScopedBatchLoader
from dagster_graphql.schema.logs.events import (
    GrapheneAssetMaterializationPlannedEvent,
    GrapheneExecutionStepFailureEvent,
    GrapheneExecutionStepStartEvent,
    GrapheneMaterializationEvent,
    GrapheneObservationEvent,
)
from rx import Observable

from dagster import DagsterInstance
from dagster import _check as check
from dagster._core.events.log import EventLogEntry
from dagster._core.host_representation import (
    ExternalRepository,
    GrpcServerRepositoryLocation,
    ManagedGrpcPythonEnvRepositoryLocationOrigin,
    RepositoryLocation,
)
from dagster._core.workspace.context import WorkspaceLocationEntry, WorkspaceLocationLoadStatus

from .asset_graph import GrapheneAssetGroup, GrapheneAssetNode
from .errors import GraphenePythonError, GrapheneRepositoryNotFoundError
from .partition_sets import GraphenePartitionSet
from .pipelines.pipeline import GrapheneJob, GraphenePipeline
from .repository_origin import GrapheneRepositoryMetadata, GrapheneRepositoryOrigin
from .schedules import GrapheneSchedule
from .sensors import GrapheneSensor
from .used_solid import GrapheneUsedSolid
from .util import non_null_list


class GrapheneLocationStateChangeEventType(graphene.Enum):
    LOCATION_UPDATED = "LOCATION_UPDATED"
    LOCATION_DISCONNECTED = "LOCATION_DISCONNECTED"
    LOCATION_RECONNECTED = "LOCATION_RECONNECTED"
    LOCATION_ERROR = "LOCATION_ERROR"

    class Meta:
        name = "LocationStateChangeEventType"


class GrapheneRepositoryLocationLoadStatus(graphene.Enum):
    LOADING = "LOADING"
    LOADED = "LOADED"

    class Meta:
        name = "RepositoryLocationLoadStatus"

    @classmethod
    def from_python_status(cls, python_status):
        check.inst_param(python_status, "python_status", WorkspaceLocationLoadStatus)
        if python_status == WorkspaceLocationLoadStatus.LOADING:
            return GrapheneRepositoryLocationLoadStatus.LOADING
        elif python_status == WorkspaceLocationLoadStatus.LOADED:
            return GrapheneRepositoryLocationLoadStatus.LOADED
        else:
            check.failed(f"Invalid location load status: {python_status}")


class GrapheneRepositoryLocation(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    is_reload_supported = graphene.NonNull(graphene.Boolean)
    environment_path = graphene.String()
    repositories = non_null_list(lambda: GrapheneRepository)
    server_id = graphene.String()

    class Meta:
        name = "RepositoryLocation"

    def __init__(self, location):
        self._location = check.inst_param(location, "location", RepositoryLocation)
        environment_path = (
            location.origin.loadable_target_origin.executable_path
            if isinstance(location.origin, ManagedGrpcPythonEnvRepositoryLocationOrigin)
            else None
        )

        server_id = (
            location.server_id if isinstance(location, GrpcServerRepositoryLocation) else None
        )

        check.invariant(location.name is not None)

        super().__init__(
            name=location.name,
            environment_path=environment_path,
            is_reload_supported=location.is_reload_supported,
            server_id=server_id,
        )

    def resolve_id(self, _):
        return self.name

    def resolve_repositories(self, graphene_info):
        return [
            GrapheneRepository(graphene_info.context.instance, repository, self._location)
            for repository in self._location.get_repositories().values()
        ]


class GrapheneRepositoryLocationOrLoadError(graphene.Union):
    class Meta:
        types = (
            GrapheneRepositoryLocation,
            GraphenePythonError,
        )
        name = "RepositoryLocationOrLoadError"


class GrapheneWorkspaceLocationEntry(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    locationOrLoadError = graphene.Field(GrapheneRepositoryLocationOrLoadError)
    loadStatus = graphene.NonNull(GrapheneRepositoryLocationLoadStatus)
    displayMetadata = non_null_list(GrapheneRepositoryMetadata)
    updatedTimestamp = graphene.NonNull(graphene.Float)

    class Meta:
        name = "WorkspaceLocationEntry"

    def __init__(self, location_entry):
        self._location_entry = check.inst_param(
            location_entry, "location_entry", WorkspaceLocationEntry
        )
        super().__init__(name=self._location_entry.origin.location_name)

    def resolve_id(self, _):
        return self.name

    def resolve_locationOrLoadError(self, _):
        if self._location_entry.repository_location:
            return GrapheneRepositoryLocation(self._location_entry.repository_location)

        error = self._location_entry.load_error
        return GraphenePythonError(error) if error else None

    def resolve_loadStatus(self, _):
        return GrapheneRepositoryLocationLoadStatus.from_python_status(
            self._location_entry.load_status
        )

    def resolve_displayMetadata(self, _):
        metadata = self._location_entry.display_metadata
        return [
            GrapheneRepositoryMetadata(key=key, value=value)
            for key, value in metadata.items()
            if value is not None
        ]

    def resolve_updatedTimestamp(self, _):
        return self._location_entry.update_timestamp


class GrapheneRepository(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    location = graphene.NonNull(GrapheneRepositoryLocation)
    pipelines = non_null_list(GraphenePipeline)
    jobs = non_null_list(GrapheneJob)
    usedSolids = graphene.Field(non_null_list(GrapheneUsedSolid))
    usedSolid = graphene.Field(GrapheneUsedSolid, name=graphene.NonNull(graphene.String))
    origin = graphene.NonNull(GrapheneRepositoryOrigin)
    partitionSets = non_null_list(GraphenePartitionSet)
    schedules = non_null_list(GrapheneSchedule)
    sensors = non_null_list(GrapheneSensor)
    assetNodes = non_null_list(GrapheneAssetNode)
    displayMetadata = non_null_list(GrapheneRepositoryMetadata)
    assetGroups = non_null_list(GrapheneAssetGroup)

    class Meta:
        name = "Repository"

    def __init__(self, instance, repository, repository_location):
        self._repository = check.inst_param(repository, "repository", ExternalRepository)
        self._repository_location = check.inst_param(
            repository_location, "repository_location", RepositoryLocation
        )
        check.inst_param(instance, "instance", DagsterInstance)
        self._batch_loader = RepositoryScopedBatchLoader(instance, repository)
        super().__init__(name=repository.name)

    def resolve_id(self, _graphene_info):
        return self._repository.get_external_origin_id()

    def resolve_origin(self, _graphene_info):
        origin = self._repository.get_external_origin()
        return GrapheneRepositoryOrigin(origin)

    def resolve_location(self, _graphene_info):
        return GrapheneRepositoryLocation(self._repository_location)

    def resolve_schedules(self, _graphene_info):
        return sorted(
            [
                GrapheneSchedule(
                    schedule,
                    self._batch_loader.get_schedule_state(schedule.name),
                    self._batch_loader,
                )
                for schedule in self._repository.get_external_schedules()
            ],
            key=lambda schedule: schedule.name,
        )

    def resolve_sensors(self, _graphene_info):
        return sorted(
            [
                GrapheneSensor(
                    sensor,
                    self._batch_loader.get_sensor_state(sensor.name),
                    self._batch_loader,
                )
                for sensor in self._repository.get_external_sensors()
            ],
            key=lambda sensor: sensor.name,
        )

    def resolve_pipelines(self, _graphene_info):
        return [
            GraphenePipeline(pipeline, self._batch_loader)
            for pipeline in sorted(
                self._repository.get_all_external_jobs(), key=lambda pipeline: pipeline.name
            )
        ]

    def resolve_jobs(self, _graphene_info):
        return [
            GrapheneJob(pipeline, self._batch_loader)
            for pipeline in sorted(
                self._repository.get_all_external_jobs(), key=lambda pipeline: pipeline.name
            )
            if pipeline.is_job
        ]

    def resolve_usedSolid(self, _graphene_info, name):
        return get_solid(self._repository, name)

    def resolve_usedSolids(self, _graphene_info):
        return get_solids(self._repository)

    def resolve_partitionSets(self, _graphene_info):
        return (
            GraphenePartitionSet(self._repository.handle, partition_set)
            for partition_set in self._repository.get_external_partition_sets()
        )

    def resolve_displayMetadata(self, _graphene_info):
        metadata = self._repository.get_display_metadata()
        return [
            GrapheneRepositoryMetadata(key=key, value=value)
            for key, value in metadata.items()
            if value is not None
        ]

    def resolve_assetNodes(self, _graphene_info):
        return [
            GrapheneAssetNode(self._repository_location, self._repository, external_asset_node)
            for external_asset_node in self._repository.get_external_asset_nodes()
        ]

    def resolve_assetGroups(self, _graphene_info):
        groups = {}
        for external_asset_node in self._repository.get_external_asset_nodes():
            if not external_asset_node.group_name:
                continue
            external_assets = groups.setdefault(external_asset_node.group_name, [])
            external_assets.append(external_asset_node)

        return [
            GrapheneAssetGroup(
                group_name, [external_node.asset_key for external_node in external_nodes]
            )
            for group_name, external_nodes in groups.items()
        ]


class GrapheneRepositoryConnection(graphene.ObjectType):
    nodes = non_null_list(GrapheneRepository)

    class Meta:
        name = "RepositoryConnection"


class GrapheneWorkspace(graphene.ObjectType):
    locationEntries = non_null_list(GrapheneWorkspaceLocationEntry)

    class Meta:
        name = "Workspace"


class GrapheneLocationStateChangeEvent(graphene.ObjectType):
    event_type = graphene.NonNull(GrapheneLocationStateChangeEventType)
    message = graphene.NonNull(graphene.String)
    location_name = graphene.NonNull(graphene.String)
    server_id = graphene.Field(graphene.String)

    class Meta:
        name = "LocationStateChangeEvent"


class GrapheneLocationStateChangeSubscription(graphene.ObjectType):
    event = graphene.Field(graphene.NonNull(GrapheneLocationStateChangeEvent))

    class Meta:
        name = "LocationStateChangeSubscription"


def get_location_state_change_observable(graphene_info):

    # This observerable lives on the process context and is never modified/destroyed, so we can
    # access it directly
    context = graphene_info.context.process_context

    return context.location_state_events.map(
        lambda event: GrapheneLocationStateChangeSubscription(
            event=GrapheneLocationStateChangeEvent(
                event_type=event.event_type,
                location_name=event.location_name,
                message=event.message,
                server_id=event.server_id,
            ),
        )
    )


class GrapheneAssetLogEventsSubscriptionEvent(graphene.Union):
    class Meta:
        types = (
            GrapheneMaterializationEvent,
            GrapheneObservationEvent,
            GrapheneAssetMaterializationPlannedEvent,
            GrapheneExecutionStepStartEvent,
            GrapheneExecutionStepFailureEvent,
        )
        name = "AssetLogEventsSubscriptionEvent"


class GrapheneAssetLogEventsSubscriptionSuccess(graphene.ObjectType):
    events = non_null_list(GrapheneAssetLogEventsSubscriptionEvent)

    class Meta:
        name = "AssetLogEventsSubscription"


class GrapheneAssetLogEventsSubscriptionFailure(graphene.ObjectType):
    message = graphene.NonNull(graphene.String)

    class Meta:
        name = "AssetLogEventsSubscriptionFailure"


class GrapheneAssetLogEventsSubscriptionPayload(graphene.Union):
    class Meta:
        types = (
            GrapheneAssetLogEventsSubscriptionSuccess,
            GrapheneAssetLogEventsSubscriptionFailure,
        )
        name = "AssetLogEventsSubscriptionPayload"


def get_asset_log_events_observable(graphene_info, asset_keys):
    instance = graphene_info.context.instance
    asset_nodes = [
        node for _, _, node in asset_node_iter(graphene_info) if node.asset_key in asset_keys
    ]

    if not asset_nodes:

        def _get_error_observable(observer):
            observer.on_next(
                GrapheneAssetLogEventsSubscriptionFailure(message="No asset nodes were specified")
            )

        return Observable.create(_get_error_observable)  # pylint: disable=E1101

    if not instance.event_log_storage.supports_watch_asset_events():

        def _get_error_observable(observer):
            observer.on_next(
                GrapheneAssetLogEventsSubscriptionFailure(
                    message="This feature is not supported by the event log storage engine"
                )
            )

        return Observable.create(_get_error_observable)  # pylint: disable=E1101

    def _handle_events(events: EventLogEntry):
        return GrapheneAssetLogEventsSubscriptionSuccess(
            events=[
                from_dagster_event_record(event, event.dagster_event.pipeline_name)
                for event in events
            ],
        )

    # pylint: disable=E1101
    return Observable.create(AssetLogsEventsSubscribe(instance, asset_nodes)).map(_handle_events)


class GrapheneRepositoriesOrError(graphene.Union):
    class Meta:
        types = (GrapheneRepositoryConnection, GraphenePythonError)
        name = "RepositoriesOrError"


class GrapheneWorkspaceOrError(graphene.Union):
    class Meta:
        types = (GrapheneWorkspace, GraphenePythonError)
        name = "WorkspaceOrError"


class GrapheneRepositoryOrError(graphene.Union):
    class Meta:
        types = (GraphenePythonError, GrapheneRepository, GrapheneRepositoryNotFoundError)
        name = "RepositoryOrError"


types = [
    GrapheneLocationStateChangeEvent,
    GrapheneLocationStateChangeEventType,
    GrapheneLocationStateChangeSubscription,
    GrapheneAssetLogEventsSubscriptionEvent,
    GrapheneAssetLogEventsSubscription,
    GrapheneRepositoriesOrError,
    GrapheneRepository,
    GrapheneRepositoryConnection,
    GrapheneRepositoryLocation,
    GrapheneRepositoryOrError,
]
