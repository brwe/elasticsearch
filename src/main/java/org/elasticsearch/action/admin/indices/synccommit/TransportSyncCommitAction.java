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

package org.elasticsearch.action.admin.indices.synccommit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.action.synccommit.TransportWriteSyncCommitAction;
import org.elasticsearch.action.synccommit.WriteSyncCommitRequest;
import org.elasticsearch.action.synccommit.WriteSyncCommitResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;


/**
 * Sync Commit Action.
 */
public class TransportSyncCommitAction extends TransportBroadcastOperationAction<SyncCommitRequest, SyncCommitResponse, ShardSyncCommitRequest, ShardSyncCommitResponse> {

    private final IndicesService indicesService;
    private final TransportWriteSyncCommitAction transportWriteSyncCommitAction;

    @Inject
    public TransportSyncCommitAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService, TransportWriteSyncCommitAction transportWriteSyncCommitAction,
                                     ActionFilters actionFilters) {
        super(settings, SyncCommitAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.indicesService = indicesService;
        this.transportWriteSyncCommitAction = transportWriteSyncCommitAction;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.FLUSH;
    }

    @Override
    protected SyncCommitRequest newRequest() {
        return new SyncCommitRequest();
    }

    @Override
    protected void doExecute(SyncCommitRequest request, ActionListener<SyncCommitResponse> listener) {
        new SyncCommitAsyncBroadcastAction(request, listener).start();
    }

    @Override
    protected SyncCommitResponse newResponse(SyncCommitRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // a non active shard, ignore
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                successfulShards++;
            }
        }
        return new SyncCommitResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, shardsResponses);
    }

    @Override
    protected ShardSyncCommitRequest newShardRequest() {
        return new ShardSyncCommitRequest();
    }

    @Override
    protected ShardSyncCommitRequest newShardRequest(int numShards, ShardRouting shard, SyncCommitRequest request) {
        return new ShardSyncCommitRequest(shard, request);
    }

    @Override
    protected ShardSyncCommitResponse newShardResponse() {
        return new ShardSyncCommitResponse();
    }

    @Override
    protected ShardSyncCommitResponse shardOperation(ShardSyncCommitRequest request) throws ElasticsearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex()).shardSafe(request.shardId().id());
        FlushRequest flushRequest = new FlushRequest().force(true).waitIfOngoing(true);
        byte[] id = indexShard.flush(flushRequest);
        return new ShardSyncCommitResponse(id, request.shardRouting());
    }

    /**
     * The sync commit request works against one primary and all of its copies.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, SyncCommitRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShardCopiesGrouped(request.shardId());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SyncCommitRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, SyncCommitRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

    private class SyncCommitAsyncBroadcastAction extends AsyncBroadcastAction {

        public SyncCommitAsyncBroadcastAction(SyncCommitRequest request, ActionListener<SyncCommitResponse> listener) {
            super(request, listener);
        }

        @Override
        protected void finishHim() {
            try {
                listener.onResponse(newResponse(request, shardsResponses, clusterState));
            } catch (Throwable e) {
                listener.onFailure(e);
            }
        }
    }
}
