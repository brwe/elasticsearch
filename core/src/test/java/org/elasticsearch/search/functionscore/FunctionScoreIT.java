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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.*;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CustomNativeScriptFactory.TestPlugin.class);
    }

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

        // TODO test with FunctionBuilder that doesn't have a name - maybe it should fail unless they all have a name
        FilterFunctionBuilder[] functionBuilders = new FilterFunctionBuilder[]{
            new FilterFunctionBuilder(matchAllQuery(), fieldValueFactorFunction("test"), "alpha"),
            new FilterFunctionBuilder(matchAllQuery(), weightFactorFunction(2f), "beta"),
        };

        QueryBuilder queryBuilder = functionScoreQuery(matchAllQuery(), functionBuilders).scoreMode(ScoreMode.SCRIPT);

        SearchResponse response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(queryBuilder)
            .get();
        assertThat(response.getHits().getAt(0).score(), equalTo(2.5f));
    }


    public static class CustomNativeScriptFactory implements NativeScriptFactory {
        public static class TestPlugin extends Plugin {
            @Override
            public String name() {
                return "mock-native-script";
            }
            @Override
            public String description() {
                return "a mock native script for testing";
            }
            public void onModule(ScriptModule scriptModule) {
                scriptModule.registerScript("custom", CustomNativeScriptFactory.class);
            }
        }
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new CustomScript(params);
        }
        @Override
        public boolean needsScores() {
            return false;
        }
    }

    static class CustomScript extends AbstractSearchScript {
        private Map<String, Object> params;
        private Map<String, Object> vars = new HashMap<>(2);

        public CustomScript(Map<String, Object> params) {
            this.params = params;
        }

        @Override
        public Object run() {
            if (vars.containsKey("ctx") && vars.get("ctx") instanceof Map) {
                Map ctx = (Map) vars.get("ctx");
                if (ctx.containsKey("_source") && ctx.get("_source") instanceof Map) {
                    Map source = (Map) ctx.get("_source");
                    source.putAll(params);
                }
            }
            // return value does not matter, the UpdateHelper class
            return null;
        }

        @Override
        public void setNextVar(String name, Object value) {
            vars.put(name, value);
        }

    }
}
