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

package org.elasticsearch.search.aggregations.reducers.bucket.union;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.bucket.BucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.slidingwindow.InternalSlidingWindow;
import org.elasticsearch.search.reducers.bucket.union.InternalUnion;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.reducers.ReducerBuilders.unionReducer;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.core.Is.is;

public class UnionReducerTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBasicUnion() throws IOException, ExecutionException, InterruptedException {
        indexData();
        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(histogram("histo").field("hist_field").interval(10).subAggregation(terms("labels").field("label_field")))
                .addReducer(unionReducer("united_labels").path("histo")).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation union = reductions.getAsMap().get("united_labels");
        assertNotNull(union);
        assertTrue(union instanceof InternalUnion);
        for (BucketReducerAggregation.Selection reducerBucket : ((InternalUnion) union).getBuckets()) {
            assertThat(reducerBucket.getBuckets().size(), is(1));
            assertThat(((MultiBucketsAggregation)((reducerBucket.getBuckets().get(0).getAggregations())).get("labels")).getBuckets().size(), is(2));
            // I was actually expecting that the union reducer reorders the selections so that it would appear to be a terms aggregation with a nested date_histogram
        }
    }

    private void indexData() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i<100; i++ ) {
            indexRequests.add(client().prepareIndex("index", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("hist_field", i)
                    .field("label_field", "label" + Integer.toString(1))
                    .endObject()));
            indexRequests.add(client().prepareIndex("index", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("hist_field", i)
                    .field("label_field", "label" + Integer.toString(2))
                    .endObject()));
        }
        indexRandom(true, true, indexRequests);
    }
}
