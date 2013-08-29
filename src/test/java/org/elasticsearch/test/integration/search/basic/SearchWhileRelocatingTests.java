/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.search.basic;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.gaussDecayFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class SearchWhileRelocatingTests extends AbstractSharedClusterTest {

    @Override
    protected int numberOfNodes() {
        return 3;
    }

    static void startAllThreads(Thread[] t) {
        for (int j = 0; j < t.length; j++) {
            t[j].start();
        }
    }

    static void joinAllThreads(Thread[] t) throws InterruptedException {
        for (int j = 0; j < t.length; j++) {
            t[j].join();
        }
    }

    @Test
    public void testSearchAndRelocateConcurrently() throws Exception {
        logger.debug("\n\n\n*************** START TEST ***********************");
        final int numShards = between(10, 11);
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("loc")
                .field("type", "geo_point").endObject().startObject("test").field("type", "string").endObject().endObject().endObject()
                .endObject().string();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
                .addMapping("type1", mapping).execute().actionGet();
        ensureYellow();
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        final int numDocs = between(90, 1000);
        for (int i = 0; i < numDocs; i++) {
            indexBuilders.add(new IndexRequestBuilder(client())
                    .setType("type")
                    .setId(Integer.toString(i))
                    .setIndex("test")
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 21)
                                    .endObject().endObject()));
        }
        indexRandom("test", true, indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]));
        client().admin().indices().prepareFlush("test").setFull(true).setRefresh(true).execute().get();
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats("test").all().get();
        final ShardStats[] shardStats = indicesStats.getShards();

        final int numIters = atLeast(10);
        for (int i = 0; i < numIters; i++) {
            logger.debug("\n\n*************** START INTERATION ***********************");
            // client().admin().indices().prepareUpdateSettings().setSettings(getExcludeSettings("test",
            // i % 2, settingsBuilder()))
            // .execute();
            int allowNodes = between(1, 3);
            logger.debug("allowing first " + allowNodes + " nodes in iteration " + i);
            allowNodes("test", between(1, 3));

            client().admin().cluster().prepareReroute().get();
            final AtomicBoolean stop = new AtomicBoolean(false);
            final List<Throwable> thrownExceptions = new CopyOnWriteArrayList<Throwable>();
            int numTreads = between(2, 10);
            final Thread[] t = new Thread[numTreads];
            logger.debug("starting " + numTreads + " threads ");
            for (int j = 0; j < numTreads; j++) {
                t[j] = new Thread() {
                    public void run() {
                        final List<Float> lonlat = new ArrayList<Float>();
                        lonlat.add(new Float(20));
                        lonlat.add(new Float(11));
                        try {
                            while (!stop.get()) {

                                SearchResponse sr = client().search(
                                        searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                                                searchSource().size(numDocs).query(
                                                        functionScoreQuery(termQuery("test", "value"),
                                                                gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                                                                CombineFunction.MULT.getName())))).get();
                                // assertThat(sr.getShardFailures().length,
                                // equalTo(0));
                                // assertThat(sr.isTimedOut(), equalTo(false));
                                if (sr.getShardFailures().length > 0) {
                                    continue;
                                }
                                

                                if (sr.getHits().totalHits() != (long) (numDocs)) {
                                    int[] numDocsInShards = new int[numShards];
                                    for (int k = 0; k < sr.getHits().hits().length; k++) {
                                        numDocsInShards[sr.getHits().hits()[k].getShard().shardId()] += 1;
                                    }
                                    for (int k = 0; k < shardStats.length; k++) {
                                        if (shardStats[k].getStats().docs.getCount() != numDocsInShards[k]) {
                                            assertThat(k, equalTo(shardStats[k].getShardId()));
                                            logger.debug("shard id :" + shardStats[k].getShardId() + " on node "
                                                    + sr.getHits().hits()[k].getShard().nodeId() + " should contain "
                                                    + shardStats[k].getStats().docs.getCount() + " but only contains " + numDocsInShards[k]);
                                        } else {
                                            logger.debug("shard id :" + k + " is on node " + sr.getHits().hits()[k].getShard().nodeId()
                                                    + sr.getHeaders());
                                        }
                                    }
                                    logger.debug("total hits {} vs hits array {}", sr.getHits().getTotalHits(),
                                            sr.getHits().getHits().length);
                                    if (sr.getSuccessfulShards() != numShards) {
                                        logger.debug("there where {} unsuccessfull shards ", numShards - sr.getSuccessfulShards());
                                    }
                                }
                                final SearchHits sh = sr.getHits();
                                
                                assertThat("total shards in search response should be equal to num shards",sr.getTotalShards(), equalTo(numShards));

                                assertHitCount(sr, (long) (numDocs));

                                assertThat("Expected hits to be the same size the actual hits array", sh.getTotalHits(),
                                        equalTo((long) (sh.getHits().length)));
                            }
                        } catch (Throwable t) {
                            if (!(t instanceof RemoteTransportException)) {
                                thrownExceptions.add(t);
                            }
                        }
                    }
                };
            }
            startAllThreads(t);
            ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0)
                    .setTimeout(new TimeValue(3000000)).execute().actionGet();
            stop.set(true);
            joinAllThreads(t);
            // check if the docs are there
            if (thrownExceptions.size() > 0) {
                List<Float> lonlat = new ArrayList<Float>();
                lonlat.add(new Float(20));
                lonlat.add(new Float(11));
                SearchResponse sr = client().search(
                        searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                                searchSource().size(numDocs).query(
                                        functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km"))
                                                .boostMode(CombineFunction.MULT.getName())))).get();
                if (sr.getHits().totalHits() == numDocs) {
                    logger.debug("The docs re appeared!");
                } else {
                    logger.debug("The docs are lost even after relocation finished!");
                }
            }
            assertThat(resp.isTimedOut(), equalTo(false));
            assertThat("failed in iteration " + i + " of " + numIters, thrownExceptions, Matchers.emptyIterable());
        }

    }

    @Test
    @Ignore
    public void testIndexStatsAndRelocateConcurrently() throws Exception {
        logger.debug("\n\n\n*************** START TEST ***********************");
        final int numShards = between(10, 11);
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("loc")
                .field("type", "geo_point").endObject().startObject("test").field("type", "string").endObject().endObject().endObject()
                .endObject().string();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
                .addMapping("type1", mapping).execute().actionGet();
        ensureYellow();
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        final int numDocs = between(90, 1000);
        for (int i = 0; i < numDocs; i++) {
            indexBuilders.add(new IndexRequestBuilder(client())
                    .setType("type")
                    .setId(Integer.toString(i))
                    .setIndex("test")
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 21)
                                    .endObject().endObject()));
        }
        indexRandom("test", true, indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]));
        client().admin().indices().prepareFlush("test").setFull(true).setRefresh(true).execute().get();
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats("test").all().get();
        final ShardStats[] shardStats = indicesStats.getShards();

        final int numIters = atLeast(10);
        for (int i = 0; i < numIters; i++) {
            logger.debug("\n\n*************** START INTERATION ***********************");
            // client().admin().indices().prepareUpdateSettings().setSettings(getExcludeSettings("test",
            // i % 2, settingsBuilder()))
            // .execute();
            int allowNodes = between(1, 3);
            logger.debug("allowing first " + allowNodes + " nodes in iteration " + i);
            allowNodes("test", between(1, 3));

            client().admin().cluster().prepareReroute().get();
            final AtomicBoolean stop = new AtomicBoolean(false);
            final List<Throwable> thrownExceptions = new CopyOnWriteArrayList<Throwable>();
            int numTreads = between(2, 10);
            final Thread[] t = new Thread[numTreads];
            logger.debug("starting " + numTreads + " threads ");
            for (int j = 0; j < numTreads; j++) {
                t[j] = new Thread() {
                    public void run() {
                        final List<Float> lonlat = new ArrayList<Float>();
                        lonlat.add(new Float(20));
                        lonlat.add(new Float(11));
                        try {
                            while (!stop.get()) {
                                IndicesStatsResponse stats = client().admin().indices().prepareStats("test").all().get();
                                if (stats.getShardFailures().length > 0) {
                                    continue;
                                }
                                assertThat("num docs ", stats.getIndex("test").getTotal().docs.getCount(), equalTo((long) numDocs));
                                assertThat("num shards and num shard stats should agree ", stats.getShards().length,
                                        equalTo(stats.getTotalShards()));
                                assertThat("num stats and num shards requested", stats.getShards().length, equalTo(numShards));
                                if (stats.getShardFailures().length > 0) {
                                    continue;
                                }
                                if (stats.getIndex("test").getTotal().docs.getCount() != numDocs) {
                                    for (int k = 0; k < stats.getShards().length; k++) {

                                        logger.debug("shard id :" + k + " on node "
                                                + stats.getShards()[k].getShardRouting().currentNodeId() + " should contain "
                                                + shardStats[k].getStats().docs.getCount() + " but only contains "
                                                + stats.getShards()[k].getStats().docs.getCount());
                                        if (stats.getShards()[k].getStats().docs.getCount() == 0) {
                                            assertThat(shardStats[k].getStats().docs.getCount(),
                                                    equalTo(stats.getShards()[k].getStats().docs.getCount()));
                                        }
                                    }
                                }
                                assertThat(stats.getIndex("test").getTotal().docs.getCount(), equalTo((long) numDocs));
                            }
                        } catch (Throwable t) {
                            if (!(t instanceof RemoteTransportException)) {
                                thrownExceptions.add(t);
                            }
                        }
                    }
                };
            }
            startAllThreads(t);
            ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0)
                    .setTimeout(new TimeValue(3000000)).execute().actionGet();
            stop.set(true);
            joinAllThreads(t);

            assertThat(resp.isTimedOut(), equalTo(false));
            assertThat("failed in iteration " + i + " of " + numIters, thrownExceptions, Matchers.emptyIterable());
        }

    }

}
