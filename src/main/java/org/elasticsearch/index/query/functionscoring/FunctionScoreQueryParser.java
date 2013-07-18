/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.functionscoring;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscoring.customscriptscoring.CustomScoreQueryParser;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class FunctionScoreQueryParser implements QueryParser {

    public static final String NAME = "custom_score";

    protected ImmutableMap<String, ScoreFunctionParser> functionParsers;

    @Inject
    public FunctionScoreQueryParser(Set<ScoreFunctionParser> parsers) {

        MapBuilder<String, ScoreFunctionParser> builder = MapBuilder.newMapBuilder();
        for (ScoreFunctionParser scoreFunctionParser : parsers) {
            for (String name : scoreFunctionParser.getNames()) {
                builder.put(name, scoreFunctionParser);
            }
        }
        this.functionParsers = builder.immutableMap();

    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        Filter filter = null;
        String currentFieldName = null;
        ScoreFunction scoreFunction = null;
        XContentParser.Token token;
        float boost = (float) 1.0;
        boolean boostFound = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else if ("filter".equals(currentFieldName)) {
                    filter = parseContext.parseInnerFilter();
                } else if ("boost".equals(currentFieldName)) {
                    boostFound = true;
                    boost = parser.floatValue();
                } else {

                    ScoreFunctionParser scoreFunctionParser = functionParsers.get(currentFieldName);
                    if (scoreFunctionParser == null) {
                        return tryParsingOldScriptScoreFunction(query, filter, boost, parser, parseContext);
                    } else {
                        scoreFunction = scoreFunctionParser.parse(parseContext, parser);
                    }
                }
            }
        }

        if (scoreFunction == null) {
            throw new QueryParsingException(parseContext.index(), "[distance_score] requires a function");
        }
        if (query == null && filter == null) {
            throw new QueryParsingException(parseContext.index(), "[distance_score] requires a 'query' or 'filter'");
        } else if (query == null) {
            query = new XConstantScoreQuery(filter);
        }
        FunctionScoreQuery fQuery = new FunctionScoreQuery(query, scoreFunction);
        if (boostFound) {
            fQuery.setBoost(boost);
        }
        return fQuery;

    }

    /**
     * This function is needed to assure backwards compatibility. The old format
     * of the script scoring was:
     * 
     * <pre>
     * {@code}
     * {
     *  "custom_score" : {
     *     "query/filter" : {
     *         ....
     *     },
     *     "params" : {
     *        ...
     *     },
     *     "lang": "script_lang",
     *     "boost" : some boost factor,
     *     "script" : "some script"
     * }
     * </pre>
     * 
     * In the new format, all parameters are supposed to be wrapped in an object
     * like this:
     * 
     * <pre>
     * {@code}
     * "custom_score" : {
     *         "query" : {
     *             ....
     *         },
     *        "script": {
     *             "params" : {
     *               ...
     *             },
     *             "lang": "script_lang",
     *             "boost" : "some ",
     *             "script" : "some script"
     *        }
     * }
     * </pre>
     * 
     * This is parsed by the {@link CustomScoreQueryParser}
     * */
    private Query tryParsingOldScriptScoreFunction(Query query, Filter filter, float boost, XContentParser parser,
            QueryParseContext parseContext) throws QueryParsingException, IOException {

        String script = null;
        String scriptLang = null;
        Map<String, Object> vars = null;

        String currentFieldName = null;
        XContentParser.Token token = parser.currentToken();
        while (token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else if ("filter".equals(currentFieldName)) {
                    filter = parseContext.parseInnerFilter();
                } else if ("params".equals(currentFieldName)) {
                    vars = parser.map();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_score] query does not support [" + currentFieldName
                            + "]");
                }
            } else if (token.isValue()) {
                if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_score] query does not support [" + currentFieldName
                            + "]");
                }
            }
        }
        if (!(query == null && filter == null)) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] requires 'query' or 'filter' field");
        }
        if (script == null) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] requires 'script' field");
        }
        if (query == null && filter == null) {
            return null;
        } else if (filter != null) {
            query = new XConstantScoreQuery(filter);
        }

        SearchScript searchScript;
        try {
            searchScript = parseContext.scriptService().search(parseContext.lookup(), scriptLang, script, vars);
        } catch (Exception e) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] the script could not be loaded", e);
        }
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(query, new CustomScoreQueryParser.ScriptScoreFunction(script, vars,
                searchScript));
        functionScoreQuery.setBoost(boost);
        return functionScoreQuery;

    }
}