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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.reducers.bucket.BucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.linearregression.slidingwindow.InternalLinearRegression;
import org.elasticsearch.search.reducers.bucket.slidingwindow.InternalSlidingWindow;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.ReducerBuilders.linearRegressionReducer;
import static org.elasticsearch.search.aggregations.ReducerBuilders.slidingWindowReducer;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.core.Is.is;

public class LinearRegressionTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBasicLinearRegression() throws IOException, ExecutionException, InterruptedException {

        double a = randomDouble() * 10 * (randomBoolean() ? -1 : 1);
        double b = randomDouble() * 10 * (randomBoolean() ? -1 : 1);
        indexData(a,b);
        double[] predictXs = {110};
        String[] xMetrics = {"x_avg"};
        String yMetric = "y_avg";

        SearchResponse searchResponse = client().prepareSearch("index").addAggregation(histogram("histo").field("hist_field").interval(1)
                .subAggregation(avg("x_avg").field("hist_field"))
                .subAggregation(avg("y_avg").field("metric_field")))
                .addReducer(linearRegressionReducer("line").path("histo").predictXs(predictXs).xMetrics(xMetrics).yMetric(yMetric))
                .get();
        assertSearchResponse(searchResponse);
        XContentBuilder builder = jsonBuilder();
        searchResponse.toXContent(builder, null);
        logger.info(builder.string());
        assertThat(((InternalLinearRegression)searchResponse.getReductions().get("line")).getParameters()[0], closeTo(a, 7.e-1)); //  <- not very accurate right now...
        assertThat(((InternalLinearRegression)searchResponse.getReductions().get("line")).getParameters()[1], closeTo(b, 7.e-1));

    }

    private void indexData(double a, double b) throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            double metricValue = i * a + b + randomGaussian();
            indexRequests.add(client().prepareIndex("index", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("hist_field", i)
                    .field("metric_field", i * a + b + randomGaussian())
                    .endObject()));
        }
        indexRandom(true, true, indexRequests);
    }
}
