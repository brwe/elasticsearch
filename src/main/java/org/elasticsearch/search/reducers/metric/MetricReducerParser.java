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

package org.elasticsearch.search.reducers.metric;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;
import org.elasticsearch.search.reducers.metric.multi.delta.Delta;
import org.elasticsearch.search.reducers.metric.multi.stats.Stats;
import org.elasticsearch.search.reducers.metric.single.avg.Avg;
import org.elasticsearch.search.reducers.metric.single.max.Max;
import org.elasticsearch.search.reducers.metric.single.min.Min;
import org.elasticsearch.search.reducers.metric.single.sum.Sum;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class MetricReducerParser implements Reducer.Parser {

    public static final ParseField BUCKETS_FIELD = new ParseField("buckets");
    public static final ParseField FIELD_NAME_FIELD = new ParseField("field");

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        String buckets = null;
        String fieldName = null;
        String opName = parser.currentName();

        XContentParser.Token token;
        String currentFieldName = null;

        Map<String, Object> parameters = new HashMap<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (BUCKETS_FIELD.match(currentFieldName)) {
                    buckets = parser.text();
                } else if (FIELD_NAME_FIELD.match(currentFieldName)) {
                    fieldName = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName
                        + "].");
            }
        }

        if (buckets == null) {
            throw new SearchParseException(context, "Missing [" + BUCKETS_FIELD.getPreferredName() + "] in " + type() + " reducer [" + reducerName + "]");
        }

        if (fieldName == null) {
            throw new SearchParseException(context, "Missing [" + FIELD_NAME_FIELD.getPreferredName() + "] in " + type() + " reducer [" + reducerName + "]");
        }

        return new MetricReducer.Factory(reducerName, buckets, fieldName, OperationFactory.get(opName, parameters));
    }
}

