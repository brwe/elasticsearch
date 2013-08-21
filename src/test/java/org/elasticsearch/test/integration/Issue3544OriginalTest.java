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

package org.elasticsearch.test.integration;


    
    import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;

import java.util.*;
     
     
    public class Issue3544OriginalTest {
     
        private static final String INDEX_NAME = "dynamic-mapping-testing";
        private static final String MAPPING_TYPE = "test-mapping";
        private static final String MAPPING_SOURCE =
            "{" +
                "\"my_type\": {" +
                    "\"date_detection\": \"false\"," +
                    "\"numeric_detection\": \"false\"," +
                    "\"dynamic\": \"true\"," +
                    "\"properties\": {" +
                        "\"an_id\": {" +
                            "\"type\": \"string\"," +
                            "\"store\": \"yes\"," +
                            "\"index\": \"not_analyzed\"" +
                        "}" +
                    "}," +
                    "\"dynamic_templates\": [" +
                        "{" +
                            "\"participants\": {" +
                                "\"path_match\": \"participants.*\"," +
                                "\"mapping\": {" +
                                    "\"type\": \"string\"," +
                                    "\"store\": \"yes\"," +
                                    "\"index\": \"analyzed\"," +
                                    "\"analyzer\": \"whitespace\"" +
                                "}" +
                            "}" +
                        "}" +
                    "]" +
                "}" +
            "}";
     
        private static final String FIELD_PREFIX = "participants.";
        private static final String[] USER_STATUS = { "ACCEPTED", "REMOVED", "PENDING" };
        private static final String TEST_USER = "test-user";
     
        private static final int NUM_DOCUMENTS = 5;
        private static final Random RANDOM = new Random(System.currentTimeMillis());
     
        private static Node node;
        private static Client client;
     
        public static void main(String[] args) throws Exception {
            // The 'fieldNames' array is used to help with retrieval of index terms after testing
            String[] fieldNames = new String[USER_STATUS.length];
     
            for (int i = 0; i < USER_STATUS.length; i++) {
                fieldNames[i] = FIELD_PREFIX + USER_STATUS[i];
            }
     
            boolean failed = false;
     
            while (!failed) {
     
                try {
                    initialiseNode();
     
                    for (int i = 0; i < NUM_DOCUMENTS; i++) {
                        indexDocument();
                    }
     
                    waitForDocs();
     
                    if (retrieveDocuments().getTotalHits() != NUM_DOCUMENTS) {
                        failed = true;
                        System.out.println("***** FAILED *****");
                    } else {
                        System.out.println("***** PASSED *****");
                    }
     
                    // Retrieve and output index terms for dynamic 'participants' fields
                    // This will highlight values that have been incorrectly tokenized
                    TermsFacet facet = client
                                        .prepareSearch(INDEX_NAME)
                                        .setQuery(QueryBuilders.matchAllQuery())
                                        .addFacet(
                                            FacetBuilders.termsFacet("facet")
                                                            .fields(fieldNames))
                                        .execute().actionGet()
                                        .getFacets().facet("facet");
     
                    for (TermsFacet.Entry entry : facet) {
                        System.out.println(entry.getTerm() + " : " + entry.getCount());
                    }
                } finally {
                    closeNode();
                }
            }
        }
     
        private static void initialiseNode() {
            Settings settings = ImmutableSettings.builder()
                                                    .put("cluster.name", "test-cluster")
                                                    .put("node.gateway.type", "none")
                                                    .put("node.http.enabled", false)
     
                                                    .put("index.store.type", "memory")
                                                    .put("index.number_of_replicas", 0)
                                                    .put("index.number_of_shards", 1)
                                                    .build();
     
            node = NodeBuilder.nodeBuilder().local(true).settings(settings).build();
            node.start();
            client = node.client();
     
            // Set up required index and mappings
            IndicesAdminClient indicesClient = client.admin().indices();
     
            if (indicesClient.prepareExists(INDEX_NAME).execute().actionGet().isExists()) {
                indicesClient.prepareDelete(INDEX_NAME).execute().actionGet();
            }
     
            indicesClient.prepareCreate(INDEX_NAME).execute().actionGet();
     
            indicesClient.preparePutMapping(INDEX_NAME)
                                .setType(MAPPING_TYPE)
                                .setSource(MAPPING_SOURCE)
                                .execute().actionGet();
        }
     
        private static void indexDocument() {
            Map<String, Object> source = new HashMap<String, Object>();
            source.put("an_id", UUID.randomUUID().toString());
     
            String status;
            List<String> participants;
     
            for (int i = 0; i < USER_STATUS.length; i++) {
              status = USER_STATUS[i];
              participants = randomParticipants(status);
     
              // Make sure every document contains the user we'll be searching for
              if (i == 0) {
                  participants.add(TEST_USER);
              }
     
              source.put(FIELD_PREFIX + status, participants);
            }
     
            client.prepareIndex(INDEX_NAME, MAPPING_TYPE)
                    .setSource(source)
                    .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
                    .execute().actionGet();
        }
     
        private static List<String> randomParticipants(String status) {
            List<String> participants = new ArrayList<String>();
            int numParticipants = RANDOM.nextInt(5);
     
            for (int i = 0; i < numParticipants; i++) {
                participants.add(status + "-" + i);
            }
     
            return participants;
        }
     
        private static void waitForDocs() throws InterruptedException {
            // Force refresh of index
            client.admin().indices().refresh(Requests.refreshRequest(INDEX_NAME)).actionGet();
            long currentCount;
     
            do {
                NodesStatsResponse response =
                    client.admin()
                            .cluster()
                            .prepareNodesStats()
                            .setIndices(new CommonStatsFlags().set(Flag.Docs, true))
                            .execute().actionGet();
     
                currentCount = response.getNodes()[0].getIndices().getDocs().getCount();
            } while (currentCount != NUM_DOCUMENTS);
        }
     
        private static SearchHits retrieveDocuments() {
            return client.prepareSearch(INDEX_NAME)
                            .setQuery(
                                QueryBuilders.matchQuery(FIELD_PREFIX + USER_STATUS[0], TEST_USER))
                            .execute().actionGet().getHits();
        }
     
        private static void closeNode() {
            node.stop();
            node.close();
        }
    }


