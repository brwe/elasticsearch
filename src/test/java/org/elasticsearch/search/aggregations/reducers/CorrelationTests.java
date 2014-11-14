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

package org.elasticsearch.search.aggregations.reducers;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.reducers.bucket.correlation.CorrelationReorderBuilder;
import org.elasticsearch.search.reducers.bucket.correlation.InternalCorrelationReorder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;

public class CorrelationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testNaiveCorrelation() throws IOException, ExecutionException, InterruptedException {
        indexDocs("reference_label");
        SearchResponse searchResponse = client().prepareSearch("index").setQuery(matchAllQuery())
                .addAggregation(terms("dummy_agg").field("dummy_field")
                        .subAggregation(terms("label").field("label")
                                .subAggregation(histogram("x").field("x").interval(1)
                                        .subAggregation(avg("avg").field("y"))))
                        .subAggregation(terms("reference_label").field("reference_label")
                                .subAggregation(histogram("x").field("x").interval(1)
                                        .subAggregation(avg("avg").field("y")))))
                .addReducer(new CorrelationReorderBuilder("corr").reference("dummy_agg.reference_label.x.avg.value").curve("dummy_agg.label.x.avg.value"))
                .get();
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(40l));
        InternalCorrelationReorder corr = searchResponse.getReductions().get("corr");
        Double[] expectedCorrelations = {1d, -1d, -1d};
        assertArrayEquals(corr.getCorrelations(), expectedCorrelations);
        assertThat(corr.getBuckets().get(0).getKey(), equalTo("label.dummy_value.1"));
    }

    @Test
    public void testNaiveCorrelationWithFilterBucket() throws IOException, ExecutionException, InterruptedException {
        indexDocs("label");
        SearchResponse searchResponse = client().prepareSearch("index").setQuery(matchAllQuery())
                .addAggregation(terms("dummy_agg").field("dummy_field")
                        .subAggregation(terms("label").field("label")
                                .subAggregation(histogram("x").field("x").interval(1)
                                        .subAggregation(avg("avg").field("y"))))
                        .subAggregation(filter("reference_label").filter(FilterBuilders.termFilter("label", "reference"))
                                .subAggregation(histogram("x").field("x").interval(1)
                                        .subAggregation(avg("avg").field("y")))))
                .addReducer(new CorrelationReorderBuilder("corr").reference("dummy_agg.reference_label.x.avg.value").curve("dummy_agg.label.x.avg.value"))
                .get();
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(40l));
        InternalCorrelationReorder corr = searchResponse.getReductions().get("corr");
        Double[] expectedCorrelations = {1d, 1d, -1d, -1d};
        assertArrayEquals(corr.getCorrelations(), expectedCorrelations);
        assertThat(corr.getBuckets().get(0).getKey(), equalTo("label.dummy_value.1"));
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
