/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.seal;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportShardSealAction extends TransportShardReplicationOperationAction<SealShardRequest, SealShardRequest, SealShardResponse> {

    public static final String ACTION_NAME = SealAction.NAME + "[s]";


    @Inject
    public TransportShardSealAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected TransportRequestOptions transportOptions() {
        return SealAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected SealShardRequest newRequestInstance() {
        return new SealShardRequest();
    }

    @Override
    protected SealShardRequest newReplicaRequestInstance() {
        return newRequestInstance();
    }

    @Override
    protected SealShardResponse newResponseInstance() {
        return new SealShardResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        String index = request.concreteIndex();
        int shardId = request.request().shardId().id();
        IndexRoutingTable routingTable = clusterState.routingTable().index(index);
        if (routingTable == null) {
            throw new IndexMissingException(new Index(index));
        }
        return routingTable.shard(shardId).shardsIt();
    }

    @Override
    protected Tuple<SealShardResponse, SealShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        decrementRefCounterAndSeal(shardRequest.request);
        return new Tuple<>(new SealShardResponse(shardRequest.shardId), shardRequest.request);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        decrementRefCounterAndSeal(shardRequest.request);
    }

    private void decrementRefCounterAndSeal(SealShardRequest shardRequest) {
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.index());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId.id());
        IndexShardState previousShardState = indexShard.changeToSealing();
        if (previousShardState == IndexShardState.SEALING || previousShardState == IndexShardState.SEALED) {
            // nothing to do, we are sealing already
            //TODO: what if the shard was in initializing, closed etc?
            return;
        }
        indexShard.decRef();
        // TODO: how to wait for the counter to go to 0 with timeout?
        while (indexShard.getNumInFlightOperations() > -1) {
        }

        //TDOD: For now just return ok, later call
        // indexShard.seal();

        // finally set to sealed
        indexShard.changeToSealed();
        ShardRouting localShardRouting = null;
        // TODO: there is probably a better way to get the shard routing
        for (ShardRouting shardRouting : clusterService.state().routingTable().index(shardRequest.index()).shard(shardRequest.shardId().id()).shards()) {
            if (shardRouting.currentNodeId().equals(clusterService.localNode().getId())) {
                localShardRouting = shardRouting;
                break;
            }
        }
       shardStateAction.shardSealed(localShardRouting, indexService.indexUUID(), "shard is sealed", clusterService.state().getNodes().masterNode());

    }

    @Override
    protected Tuple<SealShardResponse, SealShardRequest> shardOperationOnPrimaryAfterIncRef(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
        return shardOperationOnPrimary(clusterState, shardRequest);
    }

    @Override
    protected void shardOperationOnReplicaAfterIncRef(ReplicaOperationRequest shardRequest) {
        shardOperationOnReplica(shardRequest);
    }
}
