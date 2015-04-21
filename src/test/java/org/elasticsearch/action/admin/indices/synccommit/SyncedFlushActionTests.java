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

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.synccommit.TransportSyncedFlushAction;
import org.elasticsearch.action.synccommit.SyncedFlushRequest;
import org.elasticsearch.action.synccommit.SyncedFlushResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SyncedFlushActionTests extends ElasticsearchSingleNodeTest {
    final static public String INDEX = "test";
    final static public String TYPE = "test";

    @Test
    public void testSynActionResponseFailure() throws ExecutionException, InterruptedException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        int numShards = Integer.parseInt(getInstanceFromNode(ClusterService.class).state().metaData().index(INDEX).settings().get("index.number_of_shards"));
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        TransportPreSyncedFlushAction transportPreSyncedFlushAction = getInstanceFromNode(TransportPreSyncedFlushAction.class);
        // try sync on a shard which is not there
        PreSyncedFlushRequest preSyncedFlushRequest = new PreSyncedFlushRequest(new ShardId(INDEX, numShards));
        try {
            transportPreSyncedFlushAction.execute(preSyncedFlushRequest).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ElasticsearchIllegalStateException);
        }
    }

    @Test
    public void testShardSynActionResponse() throws ExecutionException, InterruptedException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        TransportPreSyncedFlushAction transportPreSyncedFlushAction = getInstanceFromNode(TransportPreSyncedFlushAction.class);
        PreSyncedShardFlushRequest syncCommitRequest = new PreSyncedShardFlushRequest(getShardRouting(), new PreSyncedFlushRequest(new ShardId(INDEX, 0)));
        PreSyncedShardFlushResponse syncCommitResponse = transportPreSyncedFlushAction.shardOperation(syncCommitRequest);
        assertArrayEquals(readCommitIdFromDisk(), syncCommitResponse.id());
    }

    // rename to SyncedFlush (WriteSyncCommitResponse)
    @Test
    public void testWriteSyncActionResponse() throws ExecutionException, InterruptedException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        client().admin().indices().prepareFlush(INDEX).get();
        TransportSyncedFlushAction transportSyncCommitAction = getInstanceFromNode(TransportSyncedFlushAction.class);
        String syncId = randomUnicodeOfLength(10);
        Map<ShardRouting, byte[]> commitIds = new HashMap<>();
        commitIds.put(getShardRouting(), readCommitIdFromDisk());
        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(new ShardId(INDEX, 0), syncId, commitIds);
        SyncedFlushResponse syncedFlushResponse = transportSyncCommitAction.execute(syncedFlushRequest).get();
        assertTrue(syncedFlushResponse.success());
        assertEquals(syncId, readSyncIdFromDisk());
        // no see if fails if commit id is wrong
        byte[] invalid = readCommitIdFromDisk();
        invalid[0] = (byte) (invalid[0] ^ Byte.MAX_VALUE);
        commitIds.put(getShardRouting(), invalid);
        String newSyncId = syncId + syncId;
        syncedFlushRequest = new SyncedFlushRequest(new ShardId(INDEX, 0), newSyncId, commitIds);
        try {
            transportSyncCommitAction.execute(syncedFlushRequest).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ElasticsearchIllegalStateException);
        }
        assertTrue(syncedFlushResponse.success());
        assertEquals(syncId, readSyncIdFromDisk());
    }

    public byte[] readCommitIdFromDisk() throws IOException {
        IndexShard indexShard = getInstanceFromNode(IndicesService.class).indexService("test").shard(0);
        Store store = indexShard.engine().config().getStore();
        SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
        return segmentInfos.getId();
    }

    public String readSyncIdFromDisk() throws IOException {
        IndexShard indexShard = getInstanceFromNode(IndicesService.class).indexService("test").shard(0);
        Store store = indexShard.engine().config().getStore();
        SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
        Map<String, String> userData = segmentInfos.getUserData();
        assertNotNull(userData.get(Engine.SYNC_COMMIT_ID));
        return userData.get(Engine.SYNC_COMMIT_ID);
    }

    public ShardRouting getShardRouting() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        return clusterService.state().routingTable().indicesRouting().get(INDEX).shard(0).primaryShard();
    }
}
