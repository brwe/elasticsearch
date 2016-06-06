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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@code field_value_factor} function in a function_score query.
 */
public class FunctionScoreIT extends ESIntegTestCase {
    /**
     * @throws IOException
     *
     * TODO:
     *  test with missing value
     *  test with parameters
     *  test with query that acts as a filter
     *  test with incorrect name (should reaise readable acception)
     *
     */
    public void testFunctionScoreWithScoreScript() throws IOException {
        assertAcked(prepareCreate("test").addMapping(
            "type1",
            jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("test")
                .field("type", "float")
                .endObject()).get());
        ensureYellow();

        client().prepareIndex("test", "type1", "1").setSource("test", 5).get();

        refresh();

        FilterFunctionBuilder[] functionBuilders = new FilterFunctionBuilder[]{
            new FilterFunctionBuilder(fieldValueFactorFunction("test")),
            new FilterFunctionBuilder(weightFactorFunction(2f))
        };

        QueryBuilder queryBuilder = functionScoreQuery(matchAllQuery(), functionBuilders).scoreMode(ScoreMode.SCRIPT);

        SearchResponse response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(queryBuilder)
            .get();
        assertThat(response.getHits().getAt(0).score(), equalTo(10f));
    }
}
