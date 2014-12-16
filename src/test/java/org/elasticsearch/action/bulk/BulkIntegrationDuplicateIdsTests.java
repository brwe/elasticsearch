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


package org.elasticsearch.action.bulk;

import com.google.common.base.Joiner;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;

public class BulkIntegrationDuplicateIdsTests extends ElasticsearchIntegrationTest {

    private AtomicBoolean stop = new AtomicBoolean(false);

    @Test
    public void testUniqueIds() throws Exception {


        final AtomicLong numDocs = new AtomicLong(0);
        final CountDownLatch indexingLatch = new CountDownLatch(1);
        final CountDownLatch rerouteLatch = new CountDownLatch(10);
        List<Thread> threads = new ArrayList();
        final int numDocsPerBulk = 10;

        for (int t = 0; t < 10; t++) {

            Thread indexingThread = new Thread() {

                @Override
                public void run() {
                    try {
                        indexingLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        XContentBuilder mapping = jsonBuilder().startObject()
                                .startObject("events")
                                .startObject("_routing")
                                .field("path", "@key")
                                .endObject()
                                .endObject();
                        client().admin().indices().prepareCreate("statistics-20141110").addMapping("events", mapping).get();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                    while (!stop.get()) {
                        BulkRequestBuilder bulkBuilder = client().prepareBulk();
                        for (int i = 0; i < numDocsPerBulk; i++) {
                            XContentBuilder doc = null;
                            try {
                                doc = jsonBuilder().startObject().field("@timestamp", "2014-11-10T14:30:00+0300").field("@key", randomRealisticUnicodeOfCodepointLength(between(0, 50))).field("@value", "149").endObject();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            bulkBuilder.add(client().prepareIndex("statistics-20141110", "events").setSource(doc));
                        }

                        BulkResponse response = bulkBuilder.get();
                        long numSuccessfullDocs = numDocsPerBulk;
                        if (response.hasFailures()) {

                            for (BulkItemResponse singleIndexRespons : response.getItems()) {
                                if (singleIndexRespons.isFailed()) {
                                    numSuccessfullDocs--;
                                }
                            }
                        }
                        numDocs.addAndGet(numSuccessfullDocs);

                        rerouteLatch.countDown();
                    }
                }
            };
            indexingThread.start();
            threads.add(indexingThread);
        }


        Thread relocationThread = new Thread() {

            @Override
            public void run() {
                try {
                    rerouteLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (int i = 0; i < 10; i++) {
                    allowNodes("statistics-20141110", between(1, cluster().numDataNodes()));
                    client().admin().cluster().prepareReroute().get();
                    ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setTimeout("5m").get();
                    logger.info("Reroute...");
                }
                stop.set(true);
            }
        };
        relocationThread.start();

        indexingLatch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        refresh();
        client().admin().indices().prepareOptimize("statistics-20141110").setMaxNumSegments(2).get();
        ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).setTimeout("5m").get();
        logger.info("Expecting {} docs", numDocs.intValue());
        SearchResponse response = client().prepareSearch("statistics-20141110").setSize(numDocs.intValue()).addField("_id").get();

        Set<String> uniqueIds = new HashSet();

        for (int i = 0; i < response.getHits().getHits().length; i++) {

            if (!uniqueIds.add(response.getHits().getHits()[i].getId())) {
                fail("duplicateIdDetected " + response.getHits().getHits()[i].getId());
            }
        }
        assertThat(response.getHits().totalHits(), equalTo(numDocs.longValue()));
        assertThat((long) uniqueIds.size(), equalTo(numDocs.longValue()));

    }

    @Test
    public void testUniqueIdsHTTP() throws Exception {


        final AtomicLong numDocs = new AtomicLong(0);
        final CountDownLatch indexingLatch = new CountDownLatch(1);
        final CountDownLatch rerouteLatch = new CountDownLatch(10);
        List<Thread> threads = new ArrayList();
        final int numDocsPerBulk = 10;

        for (int t = 0; t < 10; t++) {

            Thread indexingThread = new Thread() {

                @Override
                public void run() {
                    try {
                        try {
                            indexingLatch.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        try {
                            XContentBuilder mapping = jsonBuilder().startObject()
                                    .startObject("events")
                                    .startObject("_routing")
                                    .field("path", "@key")
                                    .endObject()
                                    .endObject();
                            client().admin().indices().prepareCreate("statistics-20141110").addMapping("events", mapping).setSettings(ImmutableSettings.builder().put("index.codec.bloom.load", false)).get();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                        while (!stop.get()) {

                            int node = randomInt(cluster().numDataNodes() - 1);

                            HttpRequestBuilder httpRequestBuilder = new HttpRequestBuilder(HttpClients.createDefault());
                            httpRequestBuilder.path("/_bulk");
                            String bulkString = "";
                            String header = "{ \"index\" : { \"_index\" : \"statistics-20141110\", \"_type\" : \"events\"} }";
                            for (int i = 0; i < numDocsPerBulk; i++) {
                                bulkString += "\n";
                                bulkString = bulkString + header + "\n";
                                bulkString = bulkString + "{\"@timestamp\":\"2014-11-10T14:30:00+0300\",\"@key\":\"" + randomAsciiOfLength(between(0, 50)) + "\",\"@value\":\"149\"}";

                            }
                            httpRequestBuilder.body(bulkString);
                            InetSocketAddress hoststring = cluster().httpAddresses()[node];
                            httpRequestBuilder.path("/_bulk");
                            httpRequestBuilder.host(hoststring.getHostName());
                            httpRequestBuilder.port(hoststring.getPort());
                            try {
                                httpRequestBuilder.method("POST");
                                HttpResponse httpResponse = httpRequestBuilder.execute();
                                String responseBody = httpResponse.getBody();
                                long numSuccessfulDocs = 0;
                                JSONObject jsonResponse = new JSONObject(responseBody);
                                if (!jsonResponse.has("errors")) {
                                    logger.info("No errors element in response : {}", jsonResponse.toString());
                                }
                                JSONArray responses = jsonResponse.getJSONArray("items");
                                for (int i = 0; i < responses.length(); i++) {
                                    JSONObject singleItemResponse = responses.getJSONObject(i).getJSONObject("create");
                                    // logger.info("Singe item response: {}", singleItemResponse);
                                    if (!singleItemResponse.has("status")) {
                                        //  logger.info("No status element in single response : {}", singleItemResponse.toString());
                                    } else if (singleItemResponse.getInt("status") != 201) {
                                    } else {
                                        numSuccessfulDocs++;
                                    }
                                }
                                numDocs.addAndGet(numSuccessfulDocs);
                                rerouteLatch.countDown();

                            } catch (Exception e) {
                                logger.info("Bulk failed due to {}, node probably restarting: {}:{}", e.getClass(), hoststring.getHostName(), hoststring.getPort());
                                //e.printStackTrace();
                            }
                        }
                    } finally {
                        rerouteLatch.countDown();
                    }
                }
            };
            indexingThread.start();
            threads.add(indexingThread);
        }


        Thread startStopThread = new Thread() {

            @Override
            public void run() {
                try {
                    try {
                        rerouteLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    for (int i = 0; i < 3; i++) {
                        try {
                            logger.info("restarting random node...");
                            ((InternalTestCluster) cluster()).restartRandomNode();
                            logger.info("...restarting done.");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        try {
                            sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    // Node node = nodeBuilder().settings(settingsBuilder().put("name", "another_node").put("cluster.name", ((InternalTestCluster) cluster()).getClusterName())).node();
                    //node.start();
                    stop.set(true);
                } finally {
                    stop.set(true);
                }
            }
        };


        Thread relocationThread = new Thread() {

            @Override
            public void run() {
                try {
                    try {
                        rerouteLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (int i = 0; i < 10; i++) {
                        try {
                            logger.info("Reroute...");
                            int notAllowedNode = between(1, cluster().numDataNodes());
                            logger.info("Relocating from node node_{}", notAllowedNode);
                            ImmutableSettings.Builder builder = ImmutableSettings.builder();
                            builder.put("index.routing.allocation.exclude._name", "node_" + notAllowedNode);
                            Settings build = builder.build();
                            client().admin().indices().prepareUpdateSettings("statistics-20141110").setSettings(build).setTimeout("5s").execute().actionGet();
                            client().admin().cluster().prepareReroute().get();
                            logger.info("sleep to wait for relocations...");
                            sleep(500);//ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setTimeout("30s").get();
                            builder = ImmutableSettings.builder();
                            builder.put("index.routing.allocation.include._name", "node_" + notAllowedNode);
                            build = builder.build();
                            client().admin().indices().prepareUpdateSettings("statistics-20141110").setSettings(build).setTimeout("5s").execute().actionGet();
                            client().admin().cluster().prepareReroute().get();
                            sleep(500);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    stop.set(true);
                } finally {
                    stop.set(true);
                }
            }

        };
        logger.info("Starting relocation thread...");
        relocationThread.start();
        logger.info("Starting start/stop thread...");
        startStopThread.start();
        logger.info("Starting indexing threads...");
        indexingLatch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }
        logger.info(" indexing done...");
        startStopThread.join();
        ((InternalTestCluster)cluster()).ensureAtLeastNumDataNodes(cluster().numDataNodes());
        logger.info(" startStopThread done...");
        //relocationThread.interrupt();
        relocationThread.join();
        logger.info(" relocationThread done...");
        refresh();

        client().admin().indices().prepareOptimize("statistics-20141110").setMaxNumSegments(2).get();

        ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).setTimeout("30s").get();
        logger.info("Expecting {} docs", numDocs.intValue());
        SearchResponse response = client().prepareSearch("statistics-20141110").setSize(numDocs.intValue() * 2).addField("_id").get();

        Set<String> uniqueIds = new HashSet();

        long dupCounter = 0;

        for (int i = 0; i < response.getHits().getHits().length; i++) {
            if (!uniqueIds.add(response.getHits().getHits()[i].getId())) {
                //fail("duplicateIdDetected " + response.getHits().getHits()[i].getId());
                dupCounter++;
            }
        }
        assertSearchResponse(response);
        logger.info("Expected {} docs and got {}", numDocs.intValue(), response.getHits().getTotalHits());
        assertThat(dupCounter, equalTo(0l));
        // assertThat(response.getHits().totalHits(), equalTo(numDocs.longValue()));

        //assertThat((long) uniqueIds.size(), equalTo(numDocs.longValue()));

    }
}
