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

package org.elasticsearch.search.aggregations.reducers.metric;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.reducers.metric.MetricResult;
import org.elasticsearch.search.reducers.metric.MetricsBuilder;
import org.elasticsearch.search.reducers.metric.SingleBucketMetricAggregation;
import org.elasticsearch.search.reducers.metric.numeric.NumericMetricResult;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.reducers.ReducerBuilders.deltaReducer;
import static org.elasticsearch.search.reducers.ReducerBuilders.statsReducer;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class MultiMetricTests extends ElasticsearchIntegrationTest {

    public static String REDUCER_NAME = "metric_name";

    @Test
    public void testVeryBasicDelta() throws IOException, ExecutionException, InterruptedException {
        indexData();
        MetricResult metric = getAndSanityCheckMetric(deltaReducer(REDUCER_NAME));
        assertThat(((NumericMetricResult) metric).getValue("value"), equalTo(18d));
        metric = getAndSanityCheckMetric(deltaReducer(REDUCER_NAME).computeGradient(true));
        assertThat(((NumericMetricResult) metric).getValue("value"), equalTo(2d));
    }

    @Test
    public void testVeryBasicStats() throws IOException, ExecutionException, InterruptedException {
        indexData();
        MetricResult metric = getAndSanityCheckMetric(statsReducer(REDUCER_NAME));
        assertThat(((NumericMetricResult) metric).getValue("length"), equalTo(10d));
    }

    private MetricResult getAndSanityCheckMetric(MetricsBuilder builder) throws IOException {
        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(histogram("histo").field("hist_field").interval(1))
                .addReducer(builder.bucketsPath("histo").field("_count")).get();
        assertSearchResponse(searchResponse);
        Aggregations reductions;
        Aggregation sumReduc;
        reductions = searchResponse.getReductions();
        sumReduc = reductions.getAsMap().get(REDUCER_NAME);
        assertNotNull(sumReduc);
        assertThat(sumReduc, instanceOf(SingleBucketMetricAggregation.class));
        sumReduc = (Aggregation)sumReduc.getProperty("histo");
        assertNotNull(sumReduc);
        assertThat(sumReduc, instanceOf(SingleBucketMetricAggregation.class));
        MetricResult metricResult = (MetricResult)sumReduc.getProperty(REDUCER_NAME);
        assertNotNull(metricResult);
        return (MetricResult) metricResult;
    }

    private void indexData() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i + 1; j++) {
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
        }
        indexRandom(true, true, indexRequests);
    }
}
