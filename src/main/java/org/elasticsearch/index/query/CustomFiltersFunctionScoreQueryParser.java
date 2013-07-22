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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MatchAllDocsFilter;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.functionscoring.ScoreFunctionParserMapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class CustomFiltersFunctionScoreQueryParser implements QueryParser {

    public static final String NAME = "custom_filters_function_score";
    ScoreFunctionParserMapper funtionParserMapper;
    @Inject
    public CustomFiltersFunctionScoreQueryParser(ScoreFunctionParserMapper funtionParserMapper) {
        this.funtionParserMapper = funtionParserMapper;
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        float boost = 1.0f;
        
        FiltersFunctionScoreQuery.ScoreMode scoreMode = FiltersFunctionScoreQuery.ScoreMode.First;
        ArrayList<FiltersFunctionScoreQuery.FilterFunction> filterFunctions = new ArrayList<FiltersFunctionScoreQuery.FilterFunction>();
        boolean filtersFound = false;
        float maxBoost = Float.MAX_VALUE;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } 
                else {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("filters".equals(currentFieldName)) {
                    filtersFound = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Filter filter = null;
                        ScoreFunction scoreFunction = null;
                        
                        
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                if ("filter".equals(currentFieldName)) {
                                    filter = parseContext.parseInnerFilter();
                                }
                                else {
                                    scoreFunction = funtionParserMapper.get(currentFieldName).parse(parseContext, parser);
                                }
                            }
                        }
                        if(filter == null) {
                            filter = new MatchAllDocsFilter();
                        }
                        filterFunctions.add(new FiltersFunctionScoreQuery.FilterFunction(filter, scoreFunction));
                        
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                 if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
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
                        throw new QueryParsingException(parseContext.index(), "[custom_filters_score] illegal score_mode [" + sScoreMode + "]");
                    }
                } else if ("max_boost".equals(currentFieldName) || "maxBoost".equals(currentFieldName)) {
                    maxBoost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (query == null) {
            throw new QueryParsingException(parseContext.index(), "[custom_filters_score] requires 'query' field");
        }
        if (!filtersFound) {
            throw new QueryParsingException(parseContext.index(), "[custom_filters_score] requires 'filters' field");
        }
        // if all filter elements returned null, just use the query
        if (filterFunctions.isEmpty()) {
            return query;
        }

       
        FiltersFunctionScoreQuery functionScoreQuery = new FiltersFunctionScoreQuery(query, scoreMode, filterFunctions.toArray(new FiltersFunctionScoreQuery.FilterFunction[filterFunctions.size()]), maxBoost);
        functionScoreQuery.setBoost(boost);
        return functionScoreQuery;
    }
}