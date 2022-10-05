import {gql, NetworkStatus, useQuery, useSubscription} from '@apollo/client';
import React from 'react';

import {useQueryRefreshAtInterval} from '../app/QueryRefresh';
import {AssetKeyInput} from '../types/globalTypes';

import {ASSET_NODE_LIVE_FRAGMENT} from './AssetNode';
import {buildLiveData} from './Utils';
import {AssetGraphLiveQuery, AssetGraphLiveQueryVariables} from './types/AssetGraphLiveQuery';
import {AssetLogEventsSubscription} from './types/AssetLogEventsSubscription';

const IDLE_POLL_RATE = 60 * 1000;
const MIN_POLL_RATE = 1 * 1000;

/** Fetches the last materialization, "upstream changed", and other live state
 * for the assets in the given pipeline or in the given set of asset keys (or both).
 *
 * Note: The "upstream changed" flag cascades, so it may not appear if the upstream
 * node that has changed is not in scope.
 */
export function useLiveDataForAssetKeys(assetKeys: AssetKeyInput[]) {
  const liveResult = useQuery<AssetGraphLiveQuery, AssetGraphLiveQueryVariables>(
    ASSETS_GRAPH_LIVE_QUERY,
    {
      skip: assetKeys.length === 0,
      variables: {assetKeys},
      notifyOnNetworkStatusChange: true,
    },
  );

  const liveDataByNode = React.useMemo(() => {
    return liveResult.data ? buildLiveData(liveResult.data) : {};
  }, [liveResult.data]);

  // Refresh the live data every 15s. This gives us the latest data,
  // but also informs us of new runs launched in other tabs that we should be
  // subscribing to for immediate updates.
  const liveDataRefreshState = useQueryRefreshAtInterval(liveResult, IDLE_POLL_RATE);

  // Subscribe to all the inProgressRunIds and optimistically update our local data
  // as we see asset events go by in the run logs
  const busy = React.useRef(false);
  busy.current = [NetworkStatus.refetch, NetworkStatus.loading].includes(liveResult.networkStatus);

  const timerRef = React.useRef<NodeJS.Timeout | null>(null);
  const onAssetEventSeen = React.useCallback(() => {
    const refetch = liveResult.refetch;
    const fire = () => {
      if (busy.current) {
        timerRef.current = setTimeout(fire, MIN_POLL_RATE);
      } else {
        timerRef.current = null;
        refetch();
      }
    };
    if (!timerRef.current) {
      timerRef.current = setTimeout(fire, MIN_POLL_RATE);
    }
    return () => {
      if (timerRef.current) {
        clearTimeout(timerRef.current);
      }
    };
  }, [timerRef, liveResult.refetch]);

  useSubscription<AssetLogEventsSubscription>(ASSET_LOG_EVENTS_SUBSCRIPTION, {
    fetchPolicy: 'no-cache',
    variables: {assetKeys},
    onSubscriptionData: onAssetEventSeen,
  });

  return {
    liveResult,
    liveDataByNode,
    liveDataRefreshState,
    assetKeys,
  };
}

// function applyAssetEvents(liveDataByNode: LiveData, events: AssetLogEvent[]) {
//   for (const event of events) {
//     const assetId =
//       'assetKey' in event && event.assetKey
//         ? toGraphId(event.assetKey)
//         : 'stepKey' in event
//         ? Object.entries(liveDataByNode).find(([_, v]) => v.stepKey === event.stepKey)?.[0]
//         : undefined;

//     if (!assetId || !(assetId in liveDataByNode)) {
//       continue;
//     }
//     const data = cloneDeep(liveDataByNode[assetId]);

//     if (event.__typename === 'AssetMaterializationPlannedEvent') {
//       if (!data.unstartedRunIds.includes(event.runId)) {
//         data.unstartedRunIds.push(event.runId);
//       }
//     }
//     if (event.__typename === 'ExecutionStepStartEvent') {
//       data.unstartedRunIds = data.unstartedRunIds.filter((r) => r !== event.runId);
//       if (!data.inProgressRunIds.includes(event.runId)) {
//         data.inProgressRunIds.push(event.runId);
//       }
//     }
//     if (event.__typename === 'ExecutionStepFailureEvent') {
//       if (data.lastMaterialization?.runId !== event.runId) {
//         data.unstartedRunIds = data.unstartedRunIds.filter((r) => r !== event.runId);
//         data.inProgressRunIds = data.inProgressRunIds.filter((r) => r !== event.runId);
//         data.runWhichFailedToMaterialize = {
//           __typename: 'Run',
//           id: event.runId,
//           status: RunStatus.STARTED,
//         };
//       }
//     }
//     if (event.__typename === 'MaterializationEvent') {
//       data.lastMaterialization = event;
//       data.computeStatus = AssetComputeStatus.UP_TO_DATE;
//       data.unstartedRunIds = data.unstartedRunIds.filter((r) => r !== event.runId);
//       data.inProgressRunIds = data.inProgressRunIds.filter((r) => r !== event.runId);
//     }

//     liveDataByNode[assetId] = data;
//   }

//   return liveDataByNode;
// }

export const ASSET_LATEST_INFO_FRAGMENT = gql`
  fragment AssetLatestInfoFragment on AssetLatestInfo {
    assetKey {
      path
    }
    computeStatus
    unstartedRunIds
    inProgressRunIds
    latestRun {
      status
      id
    }
  }
`;

const ASSETS_GRAPH_LIVE_QUERY = gql`
  query AssetGraphLiveQuery($assetKeys: [AssetKeyInput!]!) {
    assetNodes(assetKeys: $assetKeys, loadMaterializations: true) {
      id
      ...AssetNodeLiveFragment
    }
    assetsLatestInfo(assetKeys: $assetKeys) {
      ...AssetLatestInfoFragment
    }
  }

  ${ASSET_NODE_LIVE_FRAGMENT}
  ${ASSET_LATEST_INFO_FRAGMENT}
`;

const ASSET_LOG_EVENTS_SUBSCRIPTION = gql`
  subscription AssetLogEventsSubscription($assetKeys: [AssetKeyInput!]!) {
    assetLogEvents(assetKeys: $assetKeys) {
      events {
        __typename
        ... on MaterializationEvent {
          timestamp
          runId
          assetKey {
            path
          }
        }
        ... on ExecutionStepStartEvent {
          timestamp
          stepKey
          runId
        }
        ... on ExecutionStepFailureEvent {
          timestamp
          stepKey
          runId
        }
        ... on AssetMaterializationPlannedEvent {
          timestamp
          runId
          assetKey {
            path
          }
        }
      }
    }
  }
`;
