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

package org.elasticsearch.index.engine.internal;

import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.CoreMatchers.equalTo;

public class InternalEngineIntegrationTest extends ElasticsearchIntegrationTest {

    @Test
    public void testSetIndexCompoundOnFlush() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("number_of_replicas", 0).put("number_of_shards", 1)).get();
        ensureGreen();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(1, 1, "test");
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put(EngineConfig.INDEX_COMPOUND_ON_FLUSH, false)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(1, 2, "test");

        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put(EngineConfig.INDEX_COMPOUND_ON_FLUSH, true)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(2, 3, "test");
    }

    private void assertTotalCompoundSegments(int i, int t, String index) {
        IndicesSegmentResponse indicesSegmentResponse = client().admin().indices().prepareSegments(index).get();
        assertNotNull("indices segments response should contain indices", indicesSegmentResponse.getIndices());
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(index);
        assertNotNull(indexSegments);
        assertNotNull(indexSegments.getShards());
        Collection<IndexShardSegments> values = indexSegments.getShards().values();
        int compounds = 0;
        int total = 0;
        for (IndexShardSegments indexShardSegments : values) {
            for (ShardSegments s : indexShardSegments) {
                for (Segment segment : s) {
                    if (segment.isSearch() && segment.getNumDocs() > 0) {
                        if (segment.isCompound()) {
                            compounds++;
                        }
                        total++;
                    }
                }
            }
        }
        assertThat(compounds, Matchers.equalTo(i));
        assertThat(total, Matchers.equalTo(t));
    }

    private Set<Segment> segments(IndexSegments segments) {
        Set<Segment> segmentSet = new HashSet<>();
        for (IndexShardSegments s : segments) {
            for (ShardSegments shardSegments : s) {
                segmentSet.addAll(shardSegments.getSegments());
            }
        }
        return segmentSet;
    }

    @Test
    @TestLogging("org.elasticsearch.action.search:TRACE,org.elasticsearch.search:TRACE")
    public void testConcurrentIndexDeletes() throws InterruptedException {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_replicas", maximumNumberOfReplicas())).get();
        ensureGreen("test");
        final CountDownLatch latch = new CountDownLatch(1);

        List<Thread> indexThreads = new ArrayList<>();
        final IndexRequestBuilder indexRequest = client().prepareIndex().setId("1").setType("doc").setIndex("test").setSource("{\"foo\":\"bar\"}");
        final DeleteRequestBuilder deleteRequest = client().prepareDelete("test", "doc", "1");
        indexThreads.add(new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    indexRequest.get();
                } catch (Exception e) {
                    logger.info("Caught an exception while indexing", e);
                }
            }
        });
        indexThreads.add(new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    indexRequest.get();
                } catch (Exception e) {
                    logger.info("Caught an exception while indexing", e);
                }
            }
        });
        indexThreads.add(new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                deleteRequest.get();
            }
        });
        for (Thread t : indexThreads) {
            t.start();
        }
        latch.countDown();
        for (Thread t : indexThreads) {
            t.join();
        }
        flush("test");
        logger.info("Get search response from primary");
        SearchResponse primaryResponse = client().prepareSearch("test").setQuery(termQuery("_id", "1")).setPreference("_primary").setVersion(true).get();
        for (int i = 0; i< 10; i++) {
            logger.info("Get search response from other");
            SearchResponse otherResponse = client().prepareSearch("test").setQuery(termQuery("_id", "1")).setVersion(true).get();
            assertThat(primaryResponse.getHits().totalHits(), equalTo(otherResponse.getHits().totalHits()));
            if (primaryResponse.getHits().getTotalHits() == 1) {
                logger.info("Checking version of response");
                assertThat(primaryResponse.getHits().getAt(0).version(), equalTo(otherResponse.getHits().getAt(0).version()));
            }
        }

    }
}
