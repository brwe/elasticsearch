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
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptEngineService;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@code field_value_factor} function in a function_score query.
 */
public class FunctionScoreIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CustomNativeScriptFactory.TestPlugin.class, CustomNativeScriptFactoryWithDocAccess.TestPlugin.class,
            CustomNativeScriptFactoryWith_score.TestPlugin.class);
    }

    public void testFunctionScoreWithScoreScript() throws IOException {
        assertAcked(prepareCreate("test").addMapping(
            "type1",
            jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("test")
                .field("type", "float")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ).get());
        ensureYellow();

        client().prepareIndex("test", "type1", "1").setSource("test", 5).get();

        refresh();

        Map<String, Object> params = new HashMap<>();
        Script script = new Script("custom", ScriptService.ScriptType.INLINE, NativeScriptEngineService.NAME, params);

        FilterFunctionBuilder[] functionBuilders = new FilterFunctionBuilder[]{
            new FilterFunctionBuilder(matchAllQuery(), fieldValueFactorFunction("test"), "alpha", null),
            new FilterFunctionBuilder(matchAllQuery(), weightFactorFunction(2f), "beta", null),
        };


        QueryBuilder queryBuilder = functionScoreQuery(matchAllQuery(), functionBuilders, script).scoreMode(ScoreMode.SCRIPT);

        SearchResponse response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(queryBuilder)
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(2.5f));
    }

    public void testFunctionScoreWithScoreScriptAndWithNoMatchSpecified() throws IOException {
        Float noMatchScore = 9.0f;

        assertAcked(prepareCreate("test").addMapping(
            "type1",
            jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("test")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ).get());
        ensureYellow();

        client().prepareIndex("test", "type1", "1").setSource("test", "sometoken").get();

        refresh();

        Map<String, Object> params = new HashMap<>();
        Script script = new Script("custom", ScriptService.ScriptType.INLINE, NativeScriptEngineService.NAME, params);

        FilterFunctionBuilder[] functionBuilders = new FilterFunctionBuilder[]{
            new FilterFunctionBuilder(termQuery("test","someothertoken"), weightFactorFunction(7f), "alpha", noMatchScore),
            new FilterFunctionBuilder(matchAllQuery(), weightFactorFunction(2f), "beta", noMatchScore),
        };

        QueryBuilder queryBuilder = functionScoreQuery(matchAllQuery(), functionBuilders, script).scoreMode(ScoreMode.SCRIPT);

        SearchResponse response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(queryBuilder)
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(4.5f));
    }

    public static class CustomNativeScriptFactory implements NativeScriptFactory {
        public static class TestPlugin extends Plugin implements ScriptPlugin {
            @Override
            public List<NativeScriptFactory> getNativeScripts() {
                return Collections.singletonList(new CustomNativeScriptFactory());
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

        @Override
        public String getName() {
            return "custom";
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
            double alpha = ((Number) vars.get("alpha")).doubleValue();
            double beta = ((Number) vars.get("beta")).doubleValue();

            return alpha / beta;
        }

        @Override
        public void setNextVar(String name, Object value) {
            vars.put(name, value);
        }

    }

    public void testFunctionScoreWithScoreScriptWithDocAccess() throws IOException {
        assertAcked(prepareCreate("test").addMapping(
            "type1",
            jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("test")
                .field("type", "float")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ).get());
        ensureYellow();

        client().prepareIndex("test", "type1", "1").setSource("test", 5).get();

        refresh();

        Map<String, Object> params = new HashMap<>();
        Script script = new Script("custom_with_doc_access", ScriptService.ScriptType.INLINE, NativeScriptEngineService.NAME, params);

        FilterFunctionBuilder[] functionBuilders = new FilterFunctionBuilder[]{
            new FilterFunctionBuilder(matchAllQuery(), fieldValueFactorFunction("test"), "alpha", null),
            new FilterFunctionBuilder(matchAllQuery(), weightFactorFunction(2f), "beta", null),
        };

        QueryBuilder queryBuilder = functionScoreQuery(matchAllQuery(), functionBuilders, script).scoreMode(ScoreMode.SCRIPT);

        SearchResponse response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(queryBuilder)
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(12f));
    }


    public static class CustomNativeScriptFactoryWithDocAccess implements NativeScriptFactory {
        public static class TestPlugin extends Plugin implements ScriptPlugin {
            @Override
            public List<NativeScriptFactory> getNativeScripts() {
                return Collections.singletonList(new CustomNativeScriptFactoryWithDocAccess());
            }
        }

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new CustomScriptWithDocAccess(params);
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "custom_with_doc_access";
        }
    }

    static class CustomScriptWithDocAccess extends AbstractSearchScript {

        private Map<String, Object> vars = new HashMap<>();

        public CustomScriptWithDocAccess(Map<String, Object> params) {

        }

        @Override
        public Object run() {
            double score = 0;
            for (Map.Entry<String, Object> entry : vars.entrySet()) {
                score += ((Number) entry.getValue()).doubleValue();
            }
            return score + ((Number) ((ScriptDocValues) doc().get("test")).getValues().get(0)).doubleValue();
        }

        @Override
        public void setNextVar(String name, Object value) {
            vars.put(name, value);
        }

    }



    public void testFunctionScoreWithScoreScriptAnd_score() throws IOException {
        assertAcked(prepareCreate("test").addMapping(
            "type1",
            jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("test")
                .field("type", "float")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ).get());
        ensureYellow();

        client().prepareIndex("test", "type1", "1").setSource("test", 5).get();

        refresh();

        Map<String, Object> params = new HashMap<>();
        Script script = new Script("custom_with_score", ScriptService.ScriptType.INLINE, NativeScriptEngineService.NAME, params);

        FilterFunctionBuilder[] functionBuildersInner = new FilterFunctionBuilder[]{
            new FilterFunctionBuilder(matchAllQuery(), weightFactorFunction(2f))
        };
        FilterFunctionBuilder[] functionBuildersOuter = new FilterFunctionBuilder[]{
            new FilterFunctionBuilder(matchAllQuery(), weightFactorFunction(2f), "a", null),
            new FilterFunctionBuilder(matchAllQuery(), weightFactorFunction(2f), "b", null)
        };


        QueryBuilder queryBuilderInner = functionScoreQuery(matchAllQuery(), functionBuildersInner).scoreMode(ScoreMode.SUM).boostMode
            (CombineFunction.REPLACE);
        QueryBuilder queryBuilderOuter = functionScoreQuery(queryBuilderInner, functionBuildersOuter, script).scoreMode(ScoreMode.SCRIPT)
            .boostMode(CombineFunction.SUM);

        SearchResponse response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(queryBuilderOuter)
            .get();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(6f));
    }


    public static class CustomNativeScriptFactoryWith_score implements NativeScriptFactory {
        public static class TestPlugin extends Plugin implements ScriptPlugin {
            @Override
            public List<NativeScriptFactory> getNativeScripts() {
                return Collections.singletonList(new CustomNativeScriptFactoryWith_score());
            }
        }

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new CustomScriptWith_score(params);
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "custom_with_score";
        }
    }

    static class CustomScriptWith_score extends AbstractSearchScript {

        private Map<String, Object> vars = new HashMap<>(2);

        public CustomScriptWith_score(Map<String, Object> params) {

        }

        @Override
        public Object run() {
            double a = ((Number) vars.get("a")).doubleValue();
            double _score = 0;
            try {
                _score = score();
            } catch (IOException e) {
                throw new RuntimeException("score could not be accessed", e);
            }


            return a*_score;
        }

        @Override
        public void setNextVar(String name, Object value) {
            vars.put(name, value);
        }

    }
}
