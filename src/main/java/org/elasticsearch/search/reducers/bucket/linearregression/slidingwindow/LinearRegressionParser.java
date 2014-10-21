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

package org.elasticsearch.search.reducers.bucket.linearregression.slidingwindow;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LinearRegressionParser implements Reducer.Parser{

    protected static final ParseField PREDICT_XS = new ParseField("predict_xs");
    protected static final ParseField BUCKETS_FIELD = new ParseField("buckets");
    protected static final ParseField X_METRICS = new ParseField("x_metrics");
    protected static final ParseField Y_METRIC = new ParseField("y_metric");

    @Override
    public String type() {
        return InternalLinearRegression.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        
        String buckets = null;

        XContentParser.Token token;
        String currentFieldName = null;
        List<String> xMetrics = new ArrayList<>();
        String yMetric = null;
        List<Double> predictXs = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                 if (token == XContentParser.Token.VALUE_STRING) {
                    if (BUCKETS_FIELD.match(currentFieldName)) {
                        buckets = parser.text();
                    } else if (Y_METRIC.match(currentFieldName)) {
                        yMetric = parser.text();
                    } else {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token.equals(XContentParser.Token.START_ARRAY)) {
                if (X_METRICS.match(currentFieldName)) {
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        xMetrics.add(parser.text());
                    }
                } else if (PREDICT_XS.match(currentFieldName)) {
                    predictXs = new ArrayList<>();
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        predictXs.add(parser.doubleValue());
                    }

                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
            }
        }

        if (buckets == null) {
            throw new SearchParseException(context, "Missing [path] in sliding_window reducer [" + reducerName + "]");
        }
        return new LinearRegressionReducer.Factory(reducerName, buckets, xMetrics, yMetric, predictXs);
    }

}
