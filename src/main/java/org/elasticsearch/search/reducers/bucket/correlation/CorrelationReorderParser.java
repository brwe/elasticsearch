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

package org.elasticsearch.search.reducers.bucket.correlation;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CorrelationReorderParser implements Reducer.Parser {

    protected static final ParseField REFERENCE_FIELD = new ParseField("reference");
    protected static final ParseField CURVES_FIELD = new ParseField("curves");
    protected static final ParseField NUM_CORRELATING_CURVES = new ParseField("size");

    @Override
    public String type() {
        return InternalCorrelationReorder.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {


        XContentParser.Token token;
        String currentFieldName = null;
        String referenceAgg = null;
        int numCorrelatingCurves = 10;
        List<String> curves = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (REFERENCE_FIELD.match(currentFieldName)) {
                    referenceAgg = parser.text();
                } else if (NUM_CORRELATING_CURVES.match(currentFieldName)) {
                    numCorrelatingCurves = parser.intValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token.equals(XContentParser.Token.START_ARRAY)) {
                if (CURVES_FIELD.match(currentFieldName)) {
                    while (!parser.nextToken().equals(XContentParser.Token.END_ARRAY)) {
                        curves.add(parser.text());
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
            }
        }

        if (referenceAgg == null || curves.size() == 0) {
            throw new SearchParseException(context, "Missing parameter in " + type() + " reducer [" + reducerName + "]");
        }

        return new CorrelationReorderReducer.Factory(reducerName, referenceAgg, curves, numCorrelatingCurves);
    }

}
