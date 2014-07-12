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


package org.elasticsearch.search.aggregations.bucket.significant.heuristics;


import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public class ScriptHeuristic implements SignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("script_heuristic");
    private String scriptContent;
    SearchScript script = null;

    private ScriptHeuristic() {
    }

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {

            String scriptContent = in.readString();
            XContentParser scriptContentParser = XContentFactory.xContent(scriptContent).createParser(scriptContent);
            scriptContentParser.nextToken();
            scriptContentParser.nextToken();
            scriptContentParser.nextToken();
            return Guice.createInjector(new SignificantTermsHeuristicModule()).getInstance(ScriptHeuristicParser.class).parse(scriptContentParser);
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    public ScriptHeuristic(SearchScript searchScript, String scriptContent) {
        this.script = searchScript;
        this.scriptContent = scriptContent;
    }

    /**
     * Calculates score with a script
     *
     * @param subsetFreq   The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetFreq The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        script.setNextVar("_subset_freq", subsetFreq);
        script.setNextVar("_subset_size", subsetSize);
        script.setNextVar("_superset_freq", supersetFreq);
        script.setNextVar("_superset_size", supersetSize);
        return script.runAsDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        out.writeString(scriptContent);
    }

    public static class ScriptHeuristicParser implements SignificanceHeuristicParser {
        private final ScriptService scriptService;

        @Inject
        public ScriptHeuristicParser(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryParsingException {
            NAMES_FIELD.match(parser.currentName(), ParseField.EMPTY_FLAGS);
            // we need to copy the whole string and then pass it as parameter to the ScriptHeuristic
            // this is needed for transporting
            XContentBuilder scriptContent = XContentFactory.jsonBuilder();
            scriptContent.startObject();
            scriptContent.startObject(NAMES_FIELD.getPreferredName());
            parser.nextToken();
            scriptContent.copyCurrentStructure(parser);
            scriptContent.endObject();
            XContentParser scriptContentParser = XContentFactory.xContent(scriptContent.string()).createParser(scriptContent.string());

            String script = null;
            String scriptLang = null;
            XContentParser.Token token;
            Map<String, Object> params = null;
            token = scriptContentParser.nextToken();
            token = scriptContentParser.nextToken();
            token = scriptContentParser.nextToken();

            String currentFieldName = null;
            ScriptService.ScriptType scriptType = null;
            while ((token = scriptContentParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token.equals(XContentParser.Token.FIELD_NAME)) {
                    currentFieldName = scriptContentParser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("params".equals(currentFieldName)) {
                        params = scriptContentParser.map();
                    } else {
                        throw new ElasticsearchParseException("unknown object " + currentFieldName + " in script_heuristic");
                    }
                } else if (token.isValue()) {
                    if ("script".equals(currentFieldName)) {
                        script = parser.text();
                    } else if ("id".equals(currentFieldName)) {
                        script = parser.text();
                        scriptType = ScriptService.ScriptType.INDEXED;
                    } else if ("file".equals(currentFieldName)) {
                        script = parser.text();
                        scriptType = ScriptService.ScriptType.FILE;
                    } else if ("lang".equals(currentFieldName)) {
                        scriptLang = parser.text();
                    } else {
                        throw new ElasticsearchParseException("unknown field " + currentFieldName + " in script_heuristic");
                    }
                }
            }

            if (script == null) {
                throw new ElasticsearchParseException("No script found in script_heuristic");
            }

            SearchScript searchScript;
            try {
                searchScript = scriptService.search(null, scriptLang, script, scriptType, params);
            } catch (Exception e) {
                throw new ElasticsearchParseException("The script [" + script + "] could not be loaded");
            }
            return new ScriptHeuristic(searchScript, scriptContent.string());
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class ScriptHeuristicBuilder implements SignificanceHeuristicBuilder {

        private ScriptHeuristicBuilder() {
        }

        private String script;

        private String lang;

        private Map<String, Object> params = null;

        public ScriptHeuristicBuilder(String script) {
            this.script = script;
        }

        public ScriptHeuristicBuilder(String script, String lang) {
            this.script = script;
            this.lang = lang;
        }

        public ScriptHeuristicBuilder(String script, String lang, Map<String, Object> params) {
            this.script = script;
            this.lang = lang;
            this.params = params;
        }

        public ScriptHeuristicBuilder(String script, Map<String, Object> params) {
            this.script = script;
            this.params = params;
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject(STREAM.getName());
            builder.field("script", script);
            if (lang != null) {
                builder.field("lang", lang);
            }
            if (params != null) {
                builder.field("params", params);
            }
            builder.endObject();
        }
    }
}

