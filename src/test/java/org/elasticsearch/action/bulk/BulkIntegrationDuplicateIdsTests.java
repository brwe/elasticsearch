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

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class BulkIntegrationDuplicateIdsTests extends ElasticsearchIntegrationTest {

    private AtomicBoolean stop = new AtomicBoolean(false);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put(Node.HTTP_ENABLED, true).put("gateway.type", "local").build();
    }

    @Test
    public void testUpdatesHTTP() throws Exception {

        int previous_data_nodes = cluster().numDataNodes();
        final CountDownLatch updateLatch = new CountDownLatch(1);

        List<Thread> threads = new ArrayList();
        final int numDocsPerBulk = 10;
        final long numDocs = 100000;
        int numThreads = 10;
        final CountDownLatch restartLatch = new CountDownLatch(numThreads);
        assertAcked(client().admin().indices().prepareCreate("statistics-20141110").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).build()));
        ensureGreen("statistics-20141110");
        indexDocs(numDocs);

        final ConcurrentHashMap<String, String> updatedDocIds = new ConcurrentHashMap<>();

        long numDocsPerThread = numDocs/numThreads;
        for (int t = 0; t < numThreads; t++) {

            int from = (int)(numDocsPerThread)*t;
            Thread updateThread = new UpdateThread(updateLatch, numDocsPerBulk, restartLatch, numDocsPerThread, from, updatedDocIds);
            updateThread.start();
            threads.add(updateThread);
        }


        Thread startStopThread = new Thread() {

            @Override
            public void run() {
                try {
                    try {
                        restartLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (int i = 0; i < 3; i++) {
                        try {
                            logger.info("start-stop thread: try restarting node, iteration {}...", i);
                            ((InternalTestCluster) cluster()).restartRandomNode();
                            logger.info("start-stop thread, restarted node , iteration {}...", i);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        try {
                            sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    stop.set(true);
                } finally {
                    stop.set(true);
                }
            }
        };

        startStopThread.start();
        updateLatch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }
        startStopThread.join();
        logger.info("start/stop thread done");
        refresh();

        client().admin().indices().prepareOptimize("statistics-20141110").setMaxNumSegments(2).get();

        ClusterHealthResponse resp = client().admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0)
                .setWaitForNodes(Integer.toString(previous_data_nodes))
                .setWaitForGreenStatus()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout("1m")
                .get();
        logger.info(resp.toString());

        SearchResponse response = client().prepareSearch("statistics-20141110").setSize((int) numDocs).addField("counter").get();

        assertSearchResponse(response);

        for (int i = 0; i < response.getHits().getHits().length; i++) {
            assertThat(((Number) response.getHits().getHits()[i].getFields().get("counter").getValue()).intValue(), anyOf(Matchers.equalTo(0), Matchers.equalTo(1)));
        }

    }

    private void indexDocs(long numDocs) throws ExecutionException, InterruptedException, IOException {



        HttpRequestBuilder httpRequestBuilder = new HttpRequestBuilder(HttpClients.createDefault());
        httpRequestBuilder.path("/_bulk");
        String bulkString = "";

        int numDocsPerBulk = 1000;
        int numIndexedDocs = 0;
        while (numIndexedDocs<numDocs) {
            for (int i = 0; i < numDocsPerBulk; i++) {
                String docId = Integer.toString(i+numIndexedDocs);

                String header = "{ \"index\" : { \"_index\" : \"statistics-20141110\", \"_type\" : \"events\", \"_id\":\"" + docId + "\"} }";
                bulkString += "\n";
                bulkString = bulkString + header + "\n";
                bulkString = bulkString + "{\"counter\":0}";

            }
            httpRequestBuilder.body(bulkString);
            InetSocketAddress hoststring = cluster().httpAddresses()[0];
            httpRequestBuilder.path("/_bulk");
            httpRequestBuilder.host(hoststring.getHostName());
            httpRequestBuilder.port(hoststring.getPort());

            httpRequestBuilder.method("POST");
            HttpResponse httpResponse = httpRequestBuilder.execute();
            String responseBody = httpResponse.getBody();
            numIndexedDocs+=numDocsPerBulk;
        }


    }

    private class UpdateThread extends Thread {

        private final CountDownLatch restartLatch;
        private final int numDocsPerBulk;
        private final CountDownLatch updateLatch;
        private final long numDocs;
        private int from;
        ConcurrentHashMap<String, String> updatedDocIds;

        public UpdateThread(CountDownLatch updateLatch, int numDocsPerBulk, CountDownLatch restartLatch, long numDocs, int from, ConcurrentHashMap<String, String> updatedDocIds) {
            this.restartLatch = restartLatch;
            this.numDocsPerBulk = numDocsPerBulk;
            this.updateLatch = updateLatch;
            this.numDocs = numDocs;
            this.from = from;
            this.updatedDocIds = updatedDocIds;
        }

        @Override
        public void run() {
            int numUpdated = 0;
            try {
                try {
                    updateLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                String responseBody;
                while (!stop.get() &&(numUpdated < numDocs)) {

                    int node = randomInt(cluster().numDataNodes() - 1);

                    HttpRequestBuilder httpRequestBuilder = new HttpRequestBuilder(HttpClients.createDefault());
                    httpRequestBuilder.path("/_bulk");
                    String bulkString = "";

                    for (int i = 0; i < numDocsPerBulk; i++) {
                        String docId = Integer.toString(i + numUpdated + from );
                        synchronized (updatedDocIds) {
                            assertFalse(updatedDocIds.containsKey(docId));
                            updatedDocIds.put(docId, docId);
                        }
                        String header = "{ \"update\" : { \"_index\" : \"statistics-20141110\", \"_type\" : \"events\", \"_id\":\"" + docId + "\"} }";
                        bulkString += "\n";
                        bulkString = bulkString + header + "\n";
                        bulkString = bulkString + "{\"script\" : \"ctx._source.counter += 1\"}";

                    }
                    httpRequestBuilder.body(bulkString);
                    InetSocketAddress hoststring = cluster().httpAddresses()[node];
                    httpRequestBuilder.path("/_bulk");
                    httpRequestBuilder.host(hoststring.getHostName());
                    httpRequestBuilder.port(hoststring.getPort());
                    try {
                        httpRequestBuilder.method("POST");
                        HttpResponse httpResponse = httpRequestBuilder.execute();
                         responseBody = httpResponse.getBody();
                        logger.info(responseBody);

                    } catch (Exception e) {
                        logger.info("Bulk failed due to {}, node probably restarting: {}:{}", e.getClass(), hoststring.getHostName(), hoststring.getPort());
                    } finally {
                        restartLatch.countDown();
                        numUpdated += numDocsPerBulk;

                    }
                }
            } finally {
                restartLatch.countDown();
                logger.info("updated {} docs" , numUpdated);
            }
        }
    }
}
