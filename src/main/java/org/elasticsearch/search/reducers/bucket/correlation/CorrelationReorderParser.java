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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CorrelationReorderParser implements Reducer.Parser {

    protected static final ParseField REFERENCE_FIELD = new ParseField("reference");
    protected static final ParseField CURVES_FIELD = new ParseField("curves");
    protected static final ParseField NUM_CORRELATING_CURVES = new ParseField("size");
    protected static final ParseField XS_FIELD = new ParseField("x");
    protected static final ParseField YS_FIELD = new ParseField("y");

    @Override
    public String type() {
        return InternalCorrelationReorder.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {


        XContentParser.Token token;
        String currentFieldName = null;
        CurveXY referenceAgg = null;
        int numCorrelatingCurves = 10;
        List<CurveXY> curves = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NUM_CORRELATING_CURVES.match(currentFieldName)) {
                    numCorrelatingCurves = parser.intValue();
                } else{
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token.equals(XContentParser.Token.START_OBJECT)) {
                if (REFERENCE_FIELD.match(currentFieldName)) {
                    referenceAgg = parseKeyValuePaths(parser);
                } else  {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token.equals(XContentParser.Token.START_ARRAY)) {
                if (CURVES_FIELD.match(currentFieldName)) {
                    do {
                        curves.add(parseKeyValuePaths(parser));
                    } while (!parser.nextToken().equals(XContentParser.Token.END_ARRAY));
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

    private CurveXY parseKeyValuePaths(XContentParser parser) throws IOException {
        CurveXY cxy = new CurveXY();
        if (!parser.currentToken().equals(XContentParser.Token.START_OBJECT)) {
            parser.nextToken();
        }
        parseKeyValue(parser, cxy);
        parseKeyValue(parser, cxy);
        parser.nextToken();
        return cxy;
    }

    private void parseKeyValue(XContentParser parser, CurveXY cxy) throws IOException {
        parser.nextToken();
        String key = parser.currentName();
        parser.nextToken();
        String value = parser.text();
        cxy.addPath(key, value);
    }

    public static class CurveXY {
        private Map<String, String> paths = new HashMap<>();

        public CurveXY() {
        }

        public void addPath(String key, String value) {
            if (XS_FIELD.match(key) || YS_FIELD.match(key)) {
                paths.put(key, value);
            } else {
                throw new ElasticsearchParseException("Can only add x or y");
            }
        }
        public CurveXY(String xPath, String yPath) {
            paths.put("x", xPath);
            paths.put("y", yPath);
        }
        public String getXPath() {
            return paths.get("x");
        }
        public String getYPath() {
            return paths.get("y");
        }
    }
}
