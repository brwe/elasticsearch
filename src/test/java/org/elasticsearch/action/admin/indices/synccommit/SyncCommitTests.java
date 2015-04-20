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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class SyncCommitTests extends ElasticsearchIntegrationTest {

    @Test
    public void runOnce() throws InterruptedException, IOException {
        // use service here,
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.builder().put("index.number_of_replicas", internalCluster().numDataNodes() - 1)
                        .put("index.number_of_shards", 1)
                        .put("index.translog.flush_threshold_period", "1m")));
        ensureGreen("test");
        for (int j = 0; j < 10; j++) {
            client().prepareIndex("test", "test").setSource("{}").get();
        }
        SyncCommitResponse syncCommitResponse = client().admin().indices().prepareSyncCommit(new ShardId("test", 0)).setWaitIfOngoing(true).get();
        assertThat(syncCommitResponse.getFailedShards(), equalTo(0));
        assertThat(syncCommitResponse.commitIds.size(), equalTo(internalCluster().numDataNodes()));
        for (Map.Entry<ShardRouting, byte[]> entry1 : syncCommitResponse.commitIds.entrySet()) {
            for (Map.Entry<ShardRouting, byte[]> entry2 : syncCommitResponse.commitIds.entrySet()) {
                if (entry1.getKey().equals(entry2.getKey()) == false) {
                    assertFalse(entry1.getValue().equals(entry2));
                }
            }
        }
        logClusterState();

        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            IndexShard indexShard = indicesService.indexService("test").shard(0);
            Store store = indexShard.engine().config().getStore();
            SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
            Map<String, String> userData = segmentInfos.getUserData();
            assertNotNull(userData.get(Engine.SYNC_COMMIT_ID));
            assertTrue(userData.get(Engine.SYNC_COMMIT_ID).equals("123"));
        }

    }
}
