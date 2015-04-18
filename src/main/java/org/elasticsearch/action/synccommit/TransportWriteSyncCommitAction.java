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

package org.elasticsearch.action.synccommit;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 */
public class TransportWriteSyncCommitAction extends TransportShardReplicationOperationAction<WriteSyncCommitRequest, WriteSyncCommitRequest, WriteSyncCommitResponse> {

    public static final String NAME = "indices:data/synccommit";

    @Inject
    public TransportWriteSyncCommitAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                          ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected WriteSyncCommitRequest newRequestInstance() {
        return new WriteSyncCommitRequest();
    }

    @Override
    protected WriteSyncCommitRequest newReplicaRequestInstance() {
        return newRequestInstance();
    }

    @Override
    protected WriteSyncCommitResponse newResponseInstance() {
        return new WriteSyncCommitResponse();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.FLUSH;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        // get all shards for id
        return clusterService.state().routingTable().index(request.concreteIndex()).shard(request.request().shardId().id()).shardsIt();
    }

    @Override
    protected Tuple<WriteSyncCommitResponse, WriteSyncCommitRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest, IndexShard indexShard, IndexService indexService) throws Throwable {
        byte[] commitId = null;
        for (Map.Entry<ShardRouting, byte[]> entry : shardRequest.request.commitIds().entrySet()) {
            if (entry.getKey().shardsIt().nextOrNull().primary()) {
                commitId = entry.getValue();
            }
        }
        WriteSyncCommitResponse writeSyncCommitResponse = new WriteSyncCommitResponse(indexShard.syncFlushIfNoPendingChanges(shardRequest.request.syncId(), commitId));
        if (writeSyncCommitResponse.success() == false) {
            throw new ElasticsearchIllegalStateException("could not sync commit on primary");
        }
        return new Tuple<>(writeSyncCommitResponse, shardRequest.request);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest, IndexShard indexShard, IndexService indexService) {
        byte[] commitId = null;
        for (Map.Entry<ShardRouting, byte[]> entry : shardRequest.request.commitIds().entrySet()) {
            if (entry.getKey().shardsIt().nextOrNull().currentNodeId().equals(clusterService.localNode().getId())) {
                commitId = entry.getValue();
            }
        }
        logger.info("expected commit id {}", new String(commitId));
        indexShard.syncFlushIfNoPendingChanges(shardRequest.request.syncId(), commitId);
    }
}
