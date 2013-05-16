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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;

/**
 *
 */
public class RecencyScoreQueryParser implements QueryParser {

    public static final String NAME = "recency_score";

    protected final ESLogger logger;

    @Inject
    public RecencyScoreQueryParser() {

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
        String recencyScoringFunction = null; // which function to use
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
                    throw new QueryParsingException(parseContext.index(), "[recency_score] query does not support [" + currentFieldName
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
                    throw new QueryParsingException(parseContext.index(), "[recency_score] query does not support [" + currentFieldName
                            + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[recency_score] requires a 'query'");
        }
        if (scoredPointField == null) {
            throw new QueryParsingException(parseContext.index(), "[recency_score] requires a 'field'");
        }
        if (recencyScoringFunction == null) {
            throw new QueryParsingException(parseContext.index(), "[recency_score_using_script] requires a 'function'");
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

            IndexNumericFieldData<?> numericFieldData = parseContext.fieldData().getForField(mapper);
            if ("Gaussian".equals(recencyScoringFunction)) {
                functionScoreQuery = new FunctionScoreQuery(query, new RecencyScoreFunctionGauss(timeNow, std, numericFieldData));
            } else {
                throw new ElasticSearchException("Sorry, we do not have the function " + recencyScoringFunction
                        + " yet! Britta is still busy implementing the 1001 other functions, but you can add " + recencyScoringFunction
                        + " to the list...");
            }
            functionScoreQuery.setBoost(boost);

        } else if (mapper instanceof GeoPointFieldMapper) {
            throw new ElasticSearchException("Geo distance scoring not supported yet!");
        } else {
            throw new QueryParsingException(parseContext.index(), "field [" + scoredPointField
                    + "] is not a date field and also not a geo field. We do not support anything else yet.");
        }
        return functionScoreQuery;
    }

    public static class RecencyScoreFunctionGauss implements ScoreFunction {

        // private final Map<String, Object> params // maybe later give stuff
        // like std or othe rparameters to the function
        private AtomicReaderContext reader;
        long mean = -1; // mean of the gauss, is for example the user given time
                        // point
        final IndexNumericFieldData<?> timeOfDoc;
        private double std = 1.e7;
        private LongValues fieldData;

        public RecencyScoreFunctionGauss(long timePoint, double std, IndexNumericFieldData<?> timeOfDoc) {
            this.mean = timePoint;
            this.timeOfDoc = timeOfDoc;
            this.std = std;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            if (this.reader == context) { // if we are called with the same
                                          // reader, don't invalidate source
                return;
            }
            this.fieldData = this.timeOfDoc.load(context).getLongValues();
            this.reader = context;
        }

        @Override
        public float score(int docId, float subQueryScore) {

            return (float) (timeGaussScore(docId) * subQueryScore);

        }

        @Override
        public float factor(int docId) {
            return (float) timeGaussScore(docId);
        }

        private double timeGaussScore(int docId) {
            double fieldValue = fieldData.getValue(docId);
            return (float) (1.0 / (Math.sqrt(2.0 * Math.PI) * std)) * Math.exp(-0.5 * (fieldValue - this.mean) / Math.pow(std, 2.0));
        }

        @Override
        public Explanation explainScore(int docId, Explanation subQueryExpl) {
            Explanation exp;

            float score = score(docId, subQueryExpl.getValue());
            exp = new Explanation(score, "recency score function: composed of gauss with sigma^2=" + std + ":");
            exp.addDetail(subQueryExpl);
            return exp;
        }

        @Override
        public Explanation explainFactor(int docId) {
            return new Explanation(factor(docId), "exponential decay from given time point");
        }

        @Override
        public String toString() {
            return "gauss with sigma^2=" + std;
        }
    }
}