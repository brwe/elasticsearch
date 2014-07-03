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
package org.elasticsearch.search.aggregations.metrics.linearregression;

import com.google.common.primitives.Doubles;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.linearregression.sgd.SgdParser;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.search.aggregations.metrics.linearregression.sgd.SgdParser.*;

/**
 *
 */
public class LinearRegressionParser implements Aggregator.Parser {

    public static final String Y = "y";
    public static final String XS = "xs";
    public static final String PREDICT = "predict";
    public static final String DISPLAY_THETAS = "display_thetas";
    private final Map<String, RegressionMethodParser> functionParsers = new HashMap<>();

    @Override
    public String type() {
        return InternalRegression.TYPE.name();
    }



    // TODO: inject
    LinearRegressionParser() {
        SquaredRegressionParser sgdParser = new SquaredRegressionParser();
        functionParsers.put(sgdParser.getName(), sgdParser);
        LogisticRegressionParser logParser = new LogisticRegressionParser();
        functionParsers.put(logParser.getName(), logParser);
    }

    /**
     * We must override the parse method because we need to allow custom parameters
     * (execution_hint, etc) which is not possible otherwise
     */
    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ArrayList<ValuesSourceConfig<ValuesSource.Numeric>> configs; // new ValuesSourceConfig<>(ValuesSource.Numeric.class);

        String y = null;
        String script = null;
        String scriptLang = null;

        String[] xs = null;
        double[] predictXs = null;
        Map<String, Object> scriptParams = null;
        boolean displayThetas = false;
        Map<String, Object> settings = null;

        RegressionMethod.Factory regressionMethodFactory = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Y.equals(currentFieldName)) {
                    y = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else {
                    throw new SearchParseException(context, "Field " + currentFieldName + " unknown for " + aggregationName + ".");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (XS.equals(currentFieldName)) {
                    ArrayList<String> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        values.add(parser.text());
                    }
                    xs   = new String[values.size()];
                    xs = values.toArray(xs);
                } else if (PREDICT.equals(currentFieldName)) {
                    ArrayList<Double> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        values.add(parser.doubleValue());
                    }
                    predictXs = Doubles.toArray(values);
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else {
                    RegressionMethodParser functionParser = functionParsers.get(currentFieldName);
                    if (functionParser != null) {
                        regressionMethodFactory = functionParser.parse(parser, context);
                    } else {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                    }
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (DISPLAY_THETAS.equals(currentFieldName)) {
                    displayThetas = parser.booleanValue();
                } else {
                    throw  new SearchParseException(context, "Field "+ currentFieldName+ "unknown for "+ aggregationName+".");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }




        if (script != null) {
            //config.script(context.scriptService().search(context.lookup(), scriptLang, script, scriptParams));
        }

        if (y == null) {
            throw new SearchParseException(context, "y field must be specified for " + aggregationName + ".");
        }

        if (xs == null || xs.length == 0) {
            throw new SearchParseException(context, "xs must contain one or more fields" + aggregationName + ".");
        }

        if (predictXs == null || predictXs.length == 0) {
            throw new SearchParseException(context, "Predict values must be supplied for regression for " + aggregationName + ".");
        }

        if (predictXs.length != xs.length) {
            throw new SearchParseException(context, "Must have same number of inputs as prediction values for " + aggregationName + ".");
        }
        if (regressionMethodFactory == null) {
            throw new SearchParseException(context, "Must define a regression method for " + aggregationName + ".");
        }


        configs = new ArrayList<>(xs.length + 1);

        FieldMapper<?>[] mappers = new FieldMapper<?>[xs.length + 1];
        mappers[0] = context.smartNameFieldMapper(y);
        for (int i = 0; i < xs.length; i++) {
            mappers[i+1] = context.smartNameFieldMapper(xs[i]);
        }

        //if (mapper == null) {
        //    config.unmapped(true);
            //return new RegressionAggregator.Factory(aggregationName, config, regressorFactory, keyed);
        //}

        IndexFieldData<?>[] indexFieldData = new IndexFieldData<?>[xs.length + 1];

        indexFieldData[0] = context.fieldData().getForField(mappers[0]);

        ValuesSourceConfig<ValuesSource.Numeric> config = new ValuesSourceConfig<>(ValuesSource.Numeric.class);
        config.fieldContext(new FieldContext(y, indexFieldData[0], mappers[0]));
        configs.add(config);

        for (int i = 0; i < xs.length; i++) {
            indexFieldData[i+1] = context.fieldData().getForField(mappers[i+1]);
            config = new ValuesSourceConfig<>(ValuesSource.Numeric.class);
            config.fieldContext(new FieldContext(xs[i], indexFieldData[i + 1], mappers[i+1]));
            configs.add(config);
        }

        return new RegressionAggregator.Factory(aggregationName, configs, regressionMethodFactory, displayThetas, predictXs);
    }


}
