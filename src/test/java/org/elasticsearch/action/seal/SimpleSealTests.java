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

import com.google.common.base.Predicate;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.instanceOf;

/**
 */
public class SimpleSealTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testMissingIndexShardSealOperation() throws InterruptedException {
        TransportShardSealAction transportShardSealAction = getInstanceFromNode(TransportShardSealAction.class);
        SealShardRequest shardRequest = new SealShardRequest(new ShardId("test", 0));
        final CountDownLatch latch = new CountDownLatch(1);
        transportShardSealAction.execute(shardRequest, new ActionListener<SealShardResponse>() {

            @Override
            public void onResponse(SealShardResponse sealShardResponse) {
                fail("This may not be called, there is no index and validate should fail");
            }

            @Override
            public void onFailure(Throwable e) {
                logger.info("got a ", e);
                assertThat(e, instanceOf(IndexMissingException.class));
                latch.countDown();
            }
        });
        assertTrue("latch has timed out after 10 seconds", latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testShardSealOperation() throws InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        TransportShardSealAction transportShardSealAction = getInstanceFromNode(TransportShardSealAction.class);
        SealShardRequest shardRequest = new SealShardRequest(new ShardId("test", 0));
        shardRequest.index("test");
        ActionListener<SealShardResponse> listener = new ActionListener<SealShardResponse>() {
            @Override
            public void onResponse(SealShardResponse sealShardResponse) {
            }

            @Override
            public void onFailure(Throwable e) {
                logger.info("", e);
                fail("This should succeed");
            }
        };
        transportShardSealAction.execute(shardRequest, listener);
        assertTrue("counter did not go to 0", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                IndicesService indicesService = getInstanceFromNode(IndicesService.class);
                IndexService indexService = indicesService.indexServiceSafe("test");
                if (indexService.shard(0).tryIncRef() == true) {
                    indexService.shard(0).decRef();
                    return false;
                } else {
                    return true;
                }
            }
        }));
        assertTrue("shard state did not change to sealed", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                IndicesService indicesService = getInstanceFromNode(IndicesService.class);
                IndexService indexService = indicesService.indexServiceSafe("test");
                if (indexService.shard(0).state() != IndexShardState.SEALED) {
                    return false;
                } else {
                    return true;
                }
            }
        }));
        assertTrue("shard state did not change to sealed in cluster state", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setIndices("test").get();
                for (IndexShardRoutingTable indexShardRoutingTable : clusterStateResponse.getState().routingTable().index("test")) {
                    for (ShardRouting shardRouting : indexShardRoutingTable.shardsIt().asUnordered()) {
                        if (shardRouting.state() != ShardRoutingState.SEALED) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }));
    }

    @Test
    public void testSealOperation() throws InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        SealRequestBuilder sealRequestBuilder = new SealRequestBuilder(client().admin().indices(), "test");
        sealRequestBuilder.get();

        assertTrue("index state did not change to sealing in cluster state", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setIndices("test").get();
                IndexMetaData.State state = clusterStateResponse.getState().metaData().index("test").getState();
                if (state != IndexMetaData.State.SEALING && state != IndexMetaData.State.SEALED) {
                    return false;
                } else {
                    return true;
                }
            }
        }));
        assertTrue("counter did not go to 0", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                IndicesService indicesService = getInstanceFromNode(IndicesService.class);
                IndexService indexService = indicesService.indexServiceSafe("test");
                if (indexService.shard(0).tryIncRef() == true) {
                    indexService.shard(0).decRef();
                    return false;
                } else {
                    return true;
                }
            }
        }));
        assertTrue("shard state did not change to sealed", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                IndicesService indicesService = getInstanceFromNode(IndicesService.class);
                IndexService indexService = indicesService.indexServiceSafe("test");
                if (indexService.shard(0).state() != IndexShardState.SEALED) {
                    return false;
                } else {
                    return true;
                }
            }
        }));
        assertTrue("shard state did not change to sealed in cluster state", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setIndices("test").get();
                for (IndexShardRoutingTable indexShardRoutingTable : clusterStateResponse.getState().routingTable().index("test")) {
                    for (ShardRouting shardRouting : indexShardRoutingTable.shardsIt().asUnordered()) {
                        if (shardRouting.state() != ShardRoutingState.SEALED) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }));
        assertTrue("index state did not change to sealed in cluster state", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setIndices("test").get();
                IndexMetaData.State state = clusterStateResponse.getState().metaData().index("test").getState();
                if (state != IndexMetaData.State.SEALED) {
                    return false;
                } else {
                    return true;
                }
            }
        }));
    }
}
