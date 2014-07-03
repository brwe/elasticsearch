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
import org.elasticsearch.search.aggregations.metrics.linearregression.InternalRegression;
import org.elasticsearch.search.aggregations.metrics.linearregression.LinearRegressionBuilder;
import org.elasticsearch.search.aggregations.metrics.linearregression.sgd.SquaredLoss;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.lessThan;

/**
 *
 */
public class SGDTests extends ElasticsearchIntegrationTest {

    private void indexNoisyLine(String indexName, String docType, String x1field, String type, String yField, double a, double b) throws ExecutionException, InterruptedException {

        String mappings = "{\"doc\": {\"properties\":{\"x1\": {\"type\":\"" + type + "\"}, \"x2\": {\"type\":\"" + type + "\"}}}}";
        assertAcked(prepareCreate(indexName).setSettings(SETTING_NUMBER_OF_SHARDS, 2, SETTING_NUMBER_OF_REPLICAS, 0).addMapping("doc", mappings));
        String[] gb = {"0", "1"};
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            double x1 = randomDouble();
            double randgauss = randomGaussian() * 0.1;
            double y = a * x1 + b + randgauss;

            indexRequestBuilderList.add(client().prepareIndex(indexName, docType, Integer.toString(i))
                    .setSource(x1field, x1, yField, y));
            if (i%100 == 0) {
                indexRandom(true, indexRequestBuilderList);
                indexRequestBuilderList.clear();
            }
        }
        indexRandom(true, indexRequestBuilderList);
    }

    @Test
    public void testResultQuality() throws ExecutionException, InterruptedException {
        String indexName = "testidx";
        String docType = "doc";
        String x1field = "x1";
        String yField = "y";
        String type = randomBoolean() ? "float" : "double";
        double a = 1;
        double b = 0;
        indexNoisyLine(indexName, docType, x1field, type, yField, a, b);

        SearchResponse response = client().prepareSearch(indexName).setTypes(docType)
                .addAggregation(new LinearRegressionBuilder("linearregression").setY(yField).setDisplayThetas(true).setRegressionMethod(new SquaredLoss.Builder(0.5d)).setPredict(1.0f).setXs(x1field))
                .execute()
                .actionGet();
        double[] thetas = ((InternalRegression) (response.getAggregations().getAsMap().get("linearregression"))).getThetas();
        assertNotNull(thetas);
        assertThat(Math.abs(thetas[0] - b), lessThan(0.3d));
        assertThat(Math.abs(thetas[1] - a), lessThan(0.3d));
        logger.info("Thetas are {} and expected {} {}", thetas, 2, 3);
    }
}
