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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.metric.InternalMetric;
import org.elasticsearch.search.reducers.metric.MetricsBuilder;
import org.elasticsearch.search.reducers.metric.numeric.NumericMetricResult;
import org.elasticsearch.search.reducers.metric.MultiBucketMetricAggregation;
import org.elasticsearch.search.reducers.metric.SingleBucketMetricAggregation;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.reducers.ReducerBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class SimpleMetricTests extends ElasticsearchIntegrationTest {

    public static String REDUCER_NAME = "metric_name";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        indexData();
    }

    @Test
    public void testSimpleMetrics() throws IOException, ExecutionException, InterruptedException {
        ArrayList<Map<String, Double>> expectedValues = new ArrayList<>();
        expectedValues.add(new HashMap<String, Double>());
        expectedValues.get(0).put("value", 165d);
        testMetric(sumReducer(REDUCER_NAME), "sum", expectedValues);
        expectedValues.get(0).put("value", 16.5d);
        testMetric(avgReducer(REDUCER_NAME), "avg", expectedValues);
        expectedValues.get(0).put("value", 3d);
        testMetric(minReducer(REDUCER_NAME), "min", expectedValues);
        expectedValues.get(0).put("value", 30d);
        testMetric(maxReducer(REDUCER_NAME), "max", expectedValues);
    }

    @Test
    public void testBasicSum() throws IOException, ExecutionException, InterruptedException {
        ArrayList<Map<String, Double>> expectedValues = new ArrayList<>();

        expectedValues.add(new HashMap<String, Double>());
        expectedValues.add(new HashMap<String, Double>());
        expectedValues.get(0).put("value", 11d);
        expectedValues.get(1).put("value", 5.5d);
        testMetricOnTermsHisto(avgReducer(REDUCER_NAME), "avg", expectedValues);
        expectedValues.get(0).put("value", 2d);
        expectedValues.get(1).put("value", 1d);
        testMetricOnTermsHisto(minReducer(REDUCER_NAME), "min", expectedValues);
        expectedValues.get(0).put("value", 20d);
        expectedValues.get(1).put("value", 10d);
        testMetricOnTermsHisto(maxReducer(REDUCER_NAME), "max", expectedValues);
        expectedValues.get(0).put("value", 110d);
        expectedValues.get(1).put("value", 55d);
        testMetricOnTermsHisto(sumReducer(REDUCER_NAME), "sum", expectedValues);
    }


    private void testMetric(MetricsBuilder builder, String metricType, List<Map<String, Double>> expectedValues) throws IOException {

        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(histogram("histo").field("hist_field").interval(1))
                .addReducer(builder.bucketsPath("histo").field("_count")).get();
        checkResponse(searchResponse, expectedValues);

        XContentBuilder jsonRequest = jsonBuilder().startObject()
                .startObject("aggs")
                .startObject("histo")
                .startObject("histogram")
                .field("field", "hist_field")
                .field("interval", 1)
                .endObject()
                .endObject()
                .endObject()
                .startArray("reducers")
                .startObject()
                .startObject(REDUCER_NAME)
                .startObject(metricType)
                .field("buckets", "histo")
                .field("field", "_count")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        logger.info("request {}", jsonRequest.string());
        searchResponse = client().prepareSearch("index").setSource(jsonRequest).get();
        checkResponse(searchResponse, expectedValues);
    }

    private void checkResponse(SearchResponse searchResponse, List<Map<String, Double>> expectedValues) {
        assertSearchResponse(searchResponse);
        Aggregations reductions;
        Aggregation sumReduc;
        reductions = searchResponse.getReductions();
        sumReduc = reductions.getAsMap().get(REDUCER_NAME);
        assertNotNull(sumReduc);
        assertThat(sumReduc, instanceOf(SingleBucketMetricAggregation.class));
        checkValues(expectedValues, 0, ((SingleBucketMetricAggregation) sumReduc).getAggregations());
    }

    private void testMetricOnTermsHisto(MetricsBuilder builder, String metricType, List<Map<String, Double>> expectedValues) throws IOException {

        SearchResponse searchResponse = client().prepareSearch("index")
                .addAggregation(terms("labels").field("label_field").subAggregation(histogram("histo").field("hist_field").interval(1)))
                .addReducer(builder.bucketsPath("labels>histo").field("_count")).get();
        checkResponseTermsHisto(searchResponse, expectedValues);

        XContentBuilder jsonRequest = jsonBuilder().startObject()
                .startObject("aggs")
                .startObject("labels")
                .startObject("terms")
                .field("field", "label_field")
                .endObject()
                .startObject("aggs")
                .startObject("histo")
                .startObject("histogram")
                .field("field", "hist_field")
                .field("interval", 1)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startArray("reducers")
                .startObject()
                .startObject(REDUCER_NAME)
                .startObject(metricType)
                .field("buckets", "labels>histo")
                .field("field", "_count")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        logger.info("request {}", jsonRequest.string());
        searchResponse = client().prepareSearch("index").setSource(jsonRequest).get();
        checkResponseTermsHisto(searchResponse, expectedValues);
    }

    private void checkResponseTermsHisto(SearchResponse searchResponse, List<Map<String, Double>> expectedValues) {
        assertSearchResponse(searchResponse);
        Aggregations reductions = searchResponse.getReductions();
        Aggregation avgReduc = reductions.getAsMap().get("metric_name");
        assertNotNull(avgReduc);
        assertTrue(avgReduc instanceof SingleBucketMetricAggregation);
        avgReduc = (Aggregation) (avgReduc).getProperty("labels");
        assertTrue(avgReduc instanceof MultiBucketMetricAggregation);
        int expectedValueCounter = 0;
        assertThat(((MultiBucketMetricAggregation) avgReduc).getBuckets().size(), equalTo(2));
        for (MultiBucketsAggregation.Bucket bucket : ((MultiBucketMetricAggregation) avgReduc).getBuckets()) {
            assertTrue(bucket instanceof MultiBucketMetricAggregation.InternalBucket);
            assertThat(bucket.getAggregations().get("histo"), instanceOf(SingleBucketMetricAggregation.class));
            Aggregations resultAggregations = bucket.getAggregations();
            expectedValueCounter = checkValues(expectedValues, expectedValueCounter, resultAggregations);
        }
    }

    private int checkValues(List<Map<String, Double>> expectedValues, int expectedValueCounter, Aggregations resultAggregations) {

        InternalMetric metric = ((SingleBucketMetricAggregation) resultAggregations.get("histo")).getAggregations().get(REDUCER_NAME);
        for (Map.Entry<String, Double> value : expectedValues.get(expectedValueCounter).entrySet()) {
            assertThat(((NumericMetricResult)((InternalMetric)metric).getMetricResult()).getValue(value.getKey()), equalTo(value.getValue()));
        }
        expectedValueCounter++;
        return expectedValueCounter;
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
