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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MatchAllDocsFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class FunctionScoreQueryParser implements QueryParser {

    public static final String NAME = "function_score";
    ScoreFunctionParserMapper funtionParserMapper;

    @Inject
    public FunctionScoreQueryParser(ScoreFunctionParserMapper funtionParserMapper) {
        this.funtionParserMapper = funtionParserMapper;
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        float boost = 1.0f;

        FiltersFunctionScoreQuery.ScoreMode scoreMode = null;
        ArrayList<FiltersFunctionScoreQuery.FilterFunction> filterFunctions = new ArrayList<FiltersFunctionScoreQuery.FilterFunction>();
        float maxBoost = Float.MAX_VALUE;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("query".equals(currentFieldName)) {
                query = parseContext.parseInnerQuery();
            } else if ("filter".equals(currentFieldName)) {
                query = new XConstantScoreQuery(parseContext.parseInnerFilter());
            } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                scoreMode = parseScoreMode(parseContext, parser);
            } else if ("max_boost".equals(currentFieldName) || "maxBoost".equals(currentFieldName)) {
                maxBoost = parser.floatValue();
            } else if ("boost".equals(currentFieldName)) {
                boost = parser.floatValue();
            } else if ("functions".equals(currentFieldName)) {
                currentFieldName = parseFiltersAndFunctions(parseContext, parser, filterFunctions, currentFieldName);
            } else {
                filterFunctions.add(new FiltersFunctionScoreQuery.FilterFunction(null, funtionParserMapper.get(parseContext.index(),
                        currentFieldName).parse(parseContext, parser)));
            }
        }
        if (query == null) {
            throw new QueryParsingException(parseContext.index(), NAME + " requires 'query' field");
        }
        // if all filter elements returned null, just use the query
        if (filterFunctions.isEmpty()) {
            return query;
        }
        if (filterFunctions.size() == 1 && filterFunctions.get(0).filter == null) {
            if (scoreMode != null) {
                throw new ElasticSearchParseException(NAME + " cannot set the scoreMode if only one function was provided.");
            }
            if (maxBoost != Float.MAX_VALUE) {
                throw new ElasticSearchParseException(NAME + " cannot set the maxBoost if only one function was provided.");
            }
            FunctionScoreQuery theQuery = new FunctionScoreQuery(query, filterFunctions.get(0).function);
            theQuery.setBoost(boost);
            return theQuery;
        } else {
            if (scoreMode == null) {
                scoreMode = FiltersFunctionScoreQuery.ScoreMode.First;
            }
            FiltersFunctionScoreQuery functionScoreQuery = new FiltersFunctionScoreQuery(query, scoreMode,
                    filterFunctions.toArray(new FiltersFunctionScoreQuery.FilterFunction[filterFunctions.size()]), maxBoost);
            functionScoreQuery.setBoost(boost);
            return functionScoreQuery;
        }
    }

    private String parseFiltersAndFunctions(QueryParseContext parseContext, XContentParser parser,
            ArrayList<FiltersFunctionScoreQuery.FilterFunction> filterFunctions, String currentFieldName) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            Filter filter = null;
            ScoreFunction scoreFunction = null;
            if (token != XContentParser.Token.START_OBJECT) {
                throw new QueryParsingException(parseContext.index(), NAME + ": malformed filters");
            } else {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("filter".equals(currentFieldName)) {
                            filter = parseContext.parseInnerFilter();
                        } else {
                            ScoreFunctionParser functionParser = funtionParserMapper.get(parseContext.index(), currentFieldName);
                            scoreFunction = functionParser.parse(parseContext, parser);
                        }
                    }
                }
            }
            if (filter == null) {
                filter = new MatchAllDocsFilter();
            }
            filterFunctions.add(new FiltersFunctionScoreQuery.FilterFunction(filter, scoreFunction));

        }
        return currentFieldName;
    }

    private FiltersFunctionScoreQuery.ScoreMode parseScoreMode(QueryParseContext parseContext, XContentParser parser) throws IOException {
        FiltersFunctionScoreQuery.ScoreMode scoreMode;
        String sScoreMode = parser.text();
        if ("avg".equals(sScoreMode)) {
            scoreMode = FiltersFunctionScoreQuery.ScoreMode.Avg;
        } else if ("max".equals(sScoreMode)) {
            scoreMode = FiltersFunctionScoreQuery.ScoreMode.Max;
        } else if ("min".equals(sScoreMode)) {
            scoreMode = FiltersFunctionScoreQuery.ScoreMode.Min;
        } else if ("total".equals(sScoreMode)) {
            scoreMode = FiltersFunctionScoreQuery.ScoreMode.Total;
        } else if ("multiply".equals(sScoreMode)) {
            scoreMode = FiltersFunctionScoreQuery.ScoreMode.Multiply;
        } else if ("first".equals(sScoreMode)) {
            scoreMode = FiltersFunctionScoreQuery.ScoreMode.First;
        } else {
            throw new QueryParsingException(parseContext.index(), NAME + " illegal score_mode [" + sScoreMode + "]");
        }
        return scoreMode;
    }
}