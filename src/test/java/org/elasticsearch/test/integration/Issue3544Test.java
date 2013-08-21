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
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Issue3544Test extends AbstractSharedClusterTest {

    private static final String INDEX_NAME = "dynamic-mapping-testing";
    private static final String MAPPING_TYPE = "test-mapping";
    private static final String MAPPING_SOURCE = "{" + MAPPING_TYPE + ": {" + "\"properties\": {" + "\"an_id\": {"
            + "\"type\": \"string\"," + "\"store\": \"yes\"," + "\"index\": \"not_analyzed\"" + "}" + "}," + "\"dynamic_templates\": ["
            + "{" + "\"participants\": {" + "\"path_match\": \"*\"," + "\"mapping\": {" + "\"type\": \"string\"," + "\"store\": \"yes\","
            + "\"index\": \"analyzed\"," + "\"analyzer\": \"whitespace\"" + "}" + "}" + "}" + "]" + "}" + "}";

    private static final String FIELD_PREFIX = "participants.";
    private static final String USER_STATUS = "ACCEPTED";
    private static final String TEST_USER = "test-user";

    private static final int NUM_DOCUMENTS = 5;


    @Test
    public void runtTest() throws Exception {
        // The 'fieldNames' array is used to help with retrieval of index terms
        // after testing
        String fieldNames = FIELD_PREFIX + USER_STATUS;
        int count =0;
        while (count<100) {
            count ++;
            try {
                initialiseNode();
                ensureYellow();
                for (int i = 0; i < NUM_DOCUMENTS; i++) {
                    indexDocument();
                }
                refresh();
                SearchHits sh = retrieveDocuments();
                TermsFacet facet = client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery())
                        .addFacet(FacetBuilders.termsFacet("facet").fields(fieldNames)).execute().actionGet().getFacets().facet("facet");
                // Retrieve and output index terms for dynamic 'participants'
                // fields
                // This will highlight values that have been incorrectly
                // tokenized
                assertEquals(sh.getTotalHits(), NUM_DOCUMENTS);
                for (TermsFacet.Entry entry : facet) {
                    System.out.println(entry.getTerm() + " : " + entry.getCount());
                    if(entry.getTerm().string().equals("test-user")){
                        assertEquals( entry.getCount(), NUM_DOCUMENTS);
                    }
                }
            } finally {
              
            }
        }
    }

    private static void initialiseNode() {
        // Set up required index and mappings
        IndicesAdminClient indicesClient = client().admin().indices();

        if (indicesClient.prepareExists(INDEX_NAME).execute().actionGet().isExists()) {
            indicesClient.prepareDelete(INDEX_NAME).execute().actionGet();
        }
        indicesClient.prepareCreate(INDEX_NAME).addMapping(MAPPING_TYPE, MAPPING_SOURCE).execute().actionGet();
    }

    private void indexDocument() {
        Map<String, Object> source = new HashMap<String, Object>();
        source.put("an_id", UUID.randomUUID().toString());

        String status;
        status = USER_STATUS;
        source.put(FIELD_PREFIX + status, TEST_USER);
        client().prepareIndex(INDEX_NAME, MAPPING_TYPE).setSource(source).setConsistencyLevel(WriteConsistencyLevel.QUORUM).execute().actionGet();
        refresh();
    }



    private static SearchHits retrieveDocuments() {

        MatchQueryBuilder builder = QueryBuilders.matchQuery(FIELD_PREFIX + USER_STATUS, TEST_USER);
        return client().prepareSearch(INDEX_NAME).setQuery(builder).execute().actionGet().getHits();

    }

   
}