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

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@code field_value_factor} function in a function_score query.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 5)
public class TestIT extends ESIntegTestCase {
    static final String INDEX_NAME = "testidx";
    static final String DOC_TYPE = "doc";
    static final String TEXT_FIELD = "text";
    static final String CLASS_FIELD = "class";

    /**
     * Simple upgrade test for streaming significant terms buckets
     */
    //@AwaitsFix(bugUrl="https://github.com/elastic/elasticsearch/issues/13522")
    @TestLogging("_root:DEBUG,index.translog:TRACE,indices.recovery:TRACE,action.search.type:TRACE")
    public void testBucketStreaming() throws IOException, ExecutionException, InterruptedException {
        logger.debug("testBucketStreaming: indexing documents");
        String type = randomBoolean() ? "string" : "long";
        logger.info("type is {}", type);
        String settings = "{\"index.number_of_shards\": 5, \"index.number_of_replicas\": 0}";
        index01Docs(type, settings);

        logClusterState();
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).get();
        for (ShardStats shardStats : statsResponse.getIndex(INDEX_NAME).getShards()) {
            logger.info("---> shard {} has {} docs", shardStats.getShardRouting().shardId(), shardStats.getStats().getDocs().getCount());
        }
        //checkSignificantTermsAggregationCorrect();
        //int upgradedNodesCounter = 1;
            //logger.debug("testBucketStreaming: upgrading {}st node", upgradedNodesCounter++);
            ensureGreen();
            logClusterState();
            client().admin().indices().prepareForceMerge().setMaxNumSegments(1).get();
            checkSignificantTermsAggregationCorrect();
        logger.debug("testBucketStreaming: done testing significant terms while upgrading");
    }

    private void index01Docs(String type, String settings) throws ExecutionException, InterruptedException {
        String mappings = "{\"doc\": {\"properties\":{\"" + TEXT_FIELD + "\": {\"type\":\"" + type + "\"},\"" + CLASS_FIELD
                + "\": {\"type\":\"string\"}}}}";
        assertAcked(prepareCreate(INDEX_NAME).setSettings(settings).addMapping("doc", mappings));
        String[] gb = {"0", "1"};
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "1")
                .setSource(TEXT_FIELD, "1", CLASS_FIELD, "one"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "2")
                .setSource(TEXT_FIELD, "1", CLASS_FIELD, "one"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "3")
                .setSource(TEXT_FIELD, "0", CLASS_FIELD, "zero"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "4")
                .setSource(TEXT_FIELD, "0", CLASS_FIELD, "zero"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "5")
                .setSource(TEXT_FIELD, gb, CLASS_FIELD, "one"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "6")
                .setSource(TEXT_FIELD, gb, CLASS_FIELD, "zero"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "7")
                .setSource(TEXT_FIELD, "0", CLASS_FIELD, "zero"));
        indexRandom(true, indexRequestBuilderList);
    }

    private void checkSignificantTermsAggregationCorrect() {
        SearchResponse response = client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE)
                .addAggregation(new TermsBuilder("class").field(CLASS_FIELD).subAggregation(
                        new SignificantTermsBuilder("sig_terms")
                                .field(TEXT_FIELD)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits(), equalTo(7l));
        StringTerms classes = (StringTerms) response.getAggregations().get("class");
        assertThat(classes.getBuckets().size(), equalTo(2));
        for (Terms.Bucket classBucket : classes.getBuckets()) {
            Map<String, Aggregation> aggs = classBucket.getAggregations().asMap();
            assertTrue(aggs.containsKey("sig_terms"));
            SignificantTerms agg = (SignificantTerms) aggs.get("sig_terms");
            assertThat(agg.getBuckets().size(), equalTo(1));
            String term = agg.iterator().next().getKeyAsString();
            String classTerm = classBucket.getKeyAsString();
            if (classTerm.equals("zero")) {
                assertThat(term, equalTo("0"));
            } else {
                assertThat(term, equalTo("1"));
            }
        }
    }
}
