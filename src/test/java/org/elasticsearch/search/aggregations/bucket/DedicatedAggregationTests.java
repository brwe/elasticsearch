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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;


public class DedicatedAggregationTests extends ElasticsearchIntegrationTest {

    // https://github.com/elasticsearch/elasticsearch/issues/7240
    @Test
    public void testEmptyBoolIsMatchAll() throws IOException {
        String query = copyToStringFromClasspath("/org/elasticsearch/search/aggregations/bucket/agg-filter-with-empty-bool.json");
        createIndex("testidx");
        index("testidx", "apache", "1", "field", "text");
        index("testidx", "nginx", "2", "field", "text");
        refresh();
        ensureGreen("testidx");
        SearchResponse searchResponse = client().prepareSearch("testidx").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        searchResponse = client().prepareSearch("testidx").setSource(query).get();
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getAggregations().getAsMap().get("issue7240"), instanceOf(Filter.class));
        Filter filterAgg = (Filter) searchResponse.getAggregations().getAsMap().get("issue7240");
        assertThat(filterAgg.getAggregations().getAsMap().get("terms"), instanceOf(StringTerms.class));
        assertThat(((StringTerms) filterAgg.getAggregations().getAsMap().get("terms")).getBuckets().get(0).getDocCount(), equalTo(1l));
    }

    @Test
    public void testNaiveCorrelationWithFilterBucketUnit() throws IOException, ExecutionException, InterruptedException {
        indexDocs("label");
        SearchResponse searchResponse = client().prepareSearch("index").setQuery(matchAllQuery())
                .addAggregation(terms("dummy_agg").field("dummy_field")
                        .subAggregation(terms("label").field("label")
                                .subAggregation(histogram("x").field("x").interval(1)
                                        .subAggregation(avg("avg").field("y"))))
                        .subAggregation(filter("reference_label").filter(FilterBuilders.termFilter("label", "reference"))
                                .subAggregation(histogram("x").field("x").interval(1)
                                        .subAggregation(avg("avg").field("y")))))

                .get();
        //searchResponse.getAggregations().get("dummy_agg").getProperty(Arrays.asList("label.x.avg".split("\\.")));
        searchResponse.getAggregations().get("dummy_agg").getProperty("reference_label>x>avg");

    }


    private void indexDocs(String referenceLabelField) throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 10; j++) {
                int value = i == 1 ? 10 - j : j;
                indexRequests.add(client().prepareIndex().setSource(jsonBuilder().startObject().field("dummy_field", "dummy_value").field("label", Integer.toString(i)).field("y", value).field("x", j).endObject()).setIndex("index").setType("type"));
            }
        }
        for (int j = 0; j < 10; j++) {
            indexRequests.add(client().prepareIndex().setSource(jsonBuilder().startObject().field("dummy_field", "dummy_value").field("x", j).field(referenceLabelField, "reference").field("y", 9 - j).endObject()).setIndex("index").setType("type"));
        }
        indexRandom(true, indexRequests);
        ensureGreen("index");
    }
}
