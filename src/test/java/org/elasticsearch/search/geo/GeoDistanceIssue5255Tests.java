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

package org.elasticsearch.search.geo;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 */
public class GeoDistanceIssue5255Tests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleDistanceTests() throws Exception {

        //add the mapping
        String mapping = "{\n" +
                "  \"proposalsets\": {\n" +
                "    \"_parent\": {\n" +
                "      \"type\": \"service\"\n" +
                "    },\n" +
                "    \"_ttl\": {\n" +
                "      \"enabled\": true\n" +
                "    },\n" +
                "    \"properties\": {\n" +
                "      \"place\": {\n" +
                "        \"type\": \"object\",\n" +
                "        \"properties\": {\n" +
                "          \"name\": {\n" +
                "            \"type\": \"string\",\n" +
                "            \"index\": \"analyzed\",\n" +
                "            \"analyzer\": \"standard\",\n" +
                "            \"fields\": {\n" +
                "              \"raw\": {\n" +
                "                \"type\": \"string\",\n" +
                "                \"index\": \"not_analyzed\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          \"point\": {\n" +
                "            \"type\": \"geo_point\",\n" +
                "            \"fielddata\": {\n" +
                "              \"format\": \"compressed\",\n" +
                "              \"precision\": \"3m\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"city\": {\n" +
                "            \"type\": \"string\",\n" +
                "            \"index\": \"not_analyzed\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String query = "{\n" +
                "   \"query\": {\n" +
                "      \"filtered\": {\n" +
                "         \"filter\": {\n" +
                "            \"geo_distance\": {\n" +
                "               \"distance\": \"100km\",\n" +
                "               \"place.point\": {\n" +
                "                  \"lat\": 40.73,\n" +
                "                  \"lon\": -74.1\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   }\n" +
                "}";

        prepareCreate("testidx").get();
        ensureYellow("testidx");
        assertAcked(client().admin().indices().preparePutMapping("testidx").setType("proposalsets").setSource(mapping).get());

        List<IndexRequestBuilder> docBuilders = generateRandomDocs();
        try {
            indexRandom(true, true, docBuilders, true);
        } catch (MapperParsingException ex) {

        }
        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(100)
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo(1l));

        searchResponse = client().prepareSearch().setSource(query)
                .execute().actionGet();
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo(1l));
    }

    private List<IndexRequestBuilder> generateRandomDocs() {
        List<IndexRequestBuilder> docBuilders = new ArrayList<>();
        int numValidDocs = randomInt(10);
        int numMatchingDocs = randomIntBetween(1, 10);
        int numNullDocs = randomInt(10);
        int numInvalidDocs = randomInt(10);
        for (int i = 0; i < numMatchingDocs; i++) {
            String parent = randomAsciiOfLength(randomIntBetween(1, 20));
            String randomCity = randomAsciiOfLength(10);
            String randomName = randomAsciiOfLength(10);
            String doc = "{\n" +
                    "  \"place\": {" +
                    "  \"point\": {\n" +
                    "    \"lat\": 40.73,\n" +
                    "    \"lon\": -74.1\n" +
                    "  },\n" +
                    "  \"city\": \"" + randomCity + "\",\n" +
                    "  \"name\": \"" + randomName + "\"\n" +
                    "}" +
                    "}";
            docBuilders.add(client().prepareIndex("testidx", "proposalsets").setParent(parent).setSource(doc));
        }
        for (int i = 0; i < numValidDocs; i++) {
            String parent = randomAsciiOfLength(randomIntBetween(1, 20));
            double lat = 40.73 + randomDouble() * (randomBoolean() ? -1.0 : 1.0);
            double lon = 40.73 + randomDouble() * (randomBoolean() ? -1.0 : 1.0);
            String randomCity = randomAsciiOfLength(10);
            String randomName = randomAsciiOfLength(10);
            String doc = "{\n" +
                    "  \"place\": {" +
                    "  \"point\": {\n" +
                    "    \"lat\": \"" + lat + "\",\n" +
                    "    \"lon\": \"" + lon + "\"\n" +
                    "  },\n" +
                    "  \"city\": \"" + randomCity + "\",\n" +
                    "  \"name\": \"" + randomName + "\"\n" +
                    "}" +
                    "}";
            docBuilders.add(client().prepareIndex("testidx", "proposalsets").setParent(parent).setSource(doc));
        }
        for (int i = 0; i < numNullDocs; i++) {
            String parent = randomAsciiOfLength(randomIntBetween(1, 20));

            String randomCity = randomAsciiOfLength(10);
            String randomName = randomAsciiOfLength(10);
            String doc;
            if (randomBoolean()) {
                doc = "{\n" +
                        "  \"place\": {" +
                        "  \"point\": null,\n" +
                        "  \"city\": \"" + randomCity + "\",\n" +
                        "  \"name\": \"" + randomName + "\"\n" +
                        "}" +
                        "}";
            } else {
                doc = "{\n" +
                        "  \"place\": {" +
                        "  \"city\": \"" + randomCity + "\",\n" +
                        "  \"name\": \"" + randomName + "\"\n" +
                        "}" +
                        "}";
            }
            docBuilders.add(client().prepareIndex("testidx", "proposalsets").setParent(parent).setSource(doc));
        }
        for (int i = 0; i < numInvalidDocs; i++) {
            String parent = randomAsciiOfLength(randomIntBetween(1, 20));
            String lat = randomAsciiOfLength(10);
            String lon = randomAsciiOfLength(10);
            String randomCity = randomAsciiOfLength(10);
            String randomName = randomAsciiOfLength(10);
            String doc;
            if (randomBoolean()) {
                doc = "{\n" +
                        "  \"place\": {" +
                        "  \"point\": {\n" +
                        "    \"lat\": \"" + lat + "\",\n" +
                        "    \"lon\": \"" + lon + "\"\n" +
                        "  },\n" +
                        "  \"city\": \"" + randomCity + "\",\n" +
                        "  \"name\": \"" + randomName + "\"\n" +
                        "}" +
                        "}";
            } else {
                doc = "{\n" +
                        "  \"place\": {" +
                        "  \"point\": \"" + lat + "\",\n" +
                        "  \"city\": \"" + randomCity + "\",\n" +
                        "  \"name\": \"" + randomName + "\"\n" +
                        "}" +
                        "}";
            }
            docBuilders.add(client().prepareIndex("testidx", "proposalsets").setParent(parent).setSource(doc));
        }
        return docBuilders;
    }

}