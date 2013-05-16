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

import java.io.IOException;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.script.SearchScript;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 *
 */
public class CustomScoreWrapperParser implements QueryParser {

    public static final String NAME = "recency_score_wrapping_script";

    protected final ESLogger logger;

    @Inject
    public CustomScoreWrapperParser() {

        this.logger = Loggers.getLogger(RecencyScoreQueryParser.class.getName());

    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        boolean queryFound = false;
        float boost = 1.0f;
        String recencyScoringFunction = null; // which function to use, we
        String referencePoint = null; // reference point, user given
        String scoredPointField = null; // field name, here is the time/place
                                        // etc that is used for scoring
        // for scoring
        String stdField = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                    queryFound = true;
                } else {
                    throw new QueryParsingException(parseContext.index(), "[recency_score_wrapping_script] query does not support [" + currentFieldName
                            + "]");

                }
            } else if (token.isValue()) {
                if ("function".equals(currentFieldName)) {
                    recencyScoringFunction = parser.text();
                } else if ("now".equals(currentFieldName)) {
                    referencePoint = parser.text();
                } else if ("field".equals(currentFieldName)) {
                    scoredPointField = parser.text();
                } else if ("std".equals(currentFieldName)) {
                    stdField = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[recency_score_wrapping_script] query does not support ["
                            + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[recency_score_wrapping_script] requires a 'query'");
        }
        if (scoredPointField == null) {
            throw new QueryParsingException(parseContext.index(), "[recency_score_wrapping_script] requires a 'field'");
        }
        if (recencyScoringFunction == null) {
            throw new QueryParsingException(parseContext.index(), "[recency_score_wrapping_script] requires a 'function'");
        }
        if (query == null) {
            return null;
        }

        MapperService.SmartNameFieldMappers smartMappers = parseContext.smartFieldMappers(scoredPointField);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "failed to find field [" + scoredPointField + "]");
        }
        FieldMapper<?> mapper = smartMappers.mapper();
        FunctionScoreQuery functionScoreQuery;
        if (mapper instanceof DateFieldMapper) {
            DateFieldMapper dateFieldMapper = ((DateFieldMapper) mapper);
            long timeNow = parseContext.nowInMillis();
            if (referencePoint != null) {
                timeNow = dateFieldMapper.value(referencePoint).longValue();
            }

            // now, try to get the std from the parameters
            long std = (long) 1.e7;
            if (stdField != null) {
                DateTimeFormatter formatter = ISODateTimeFormat.basicTime();
                std = formatter.parseMillis(stdField);
            }

            String script = null;
            if ("Gaussian".equals(recencyScoringFunction)) {
                script = "_score *1/(sqrt(2.0 * " + Math.PI + ")*" + std + ")*exp(-0.5*(doc[\"" + scoredPointField
                        + "\"].date.getMillis()-" + timeNow + ")/" + std*std + ")";
            } else {
                throw new ElasticSearchException("Sorry, we do not have the function " + recencyScoringFunction
                        + " yet! Britta is still busy implementing the 1001 other functions, but you can add " + recencyScoringFunction
                        + " to the list...");
            }

            SearchScript searchScript = parseContext.scriptService().search(parseContext.lookup(), null, script, null);
            functionScoreQuery = new FunctionScoreQuery(query, new CustomScoreQueryParser.ScriptScoreFunction(script, null, searchScript));
            functionScoreQuery.setBoost(boost);

        } else if (mapper instanceof GeoPointFieldMapper) {
            throw new ElasticSearchException("Geo distance scoring not supported yet!");
        } else {
            throw new QueryParsingException(parseContext.index(), "field [" + scoredPointField
                    + "] is not a date field and also not a geo field. We do not support anything else yet.");
        }
        return functionScoreQuery;
    }

}