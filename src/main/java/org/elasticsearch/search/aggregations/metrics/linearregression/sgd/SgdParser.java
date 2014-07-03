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
package org.elasticsearch.search.aggregations.metrics.linearregression.sgd;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.metrics.linearregression.*;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 */
public class SgdParser implements RegressionMethodParser {

    public static final String REGRESSOR = "regressor";
    public static final String Y = "y";
    public static final String XS = "xs";
    public static final String PREDICT = "predict";
    public static final String DISPLAY_THETAS = "display_thetas";
    public static final String ALPHA = "alpha";

    @Override
    public String type() {
        return InternalRegression.TYPE.name();
    }

    public RegressionMethod.Factory parse(XContentParser parser, SearchContext context) throws IOException {

        double alpha = 0.5;
        String lossFunction = "squared";
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            if(currentFieldName.equals("alpha")) {
                parser.nextToken();
                alpha = parser.doubleValue();

            }
            if(currentFieldName.equals("loss_function")) {
                parser.nextToken();
                lossFunction = parser.text();
            }
        }
        if (lossFunction == null) {
            lossFunction = "squared";
        }
        return  RegressorType.resolve(lossFunction, context).regressorFactory(alpha);
    }

    /**
     *
     */
    public static enum RegressorType {
        SQUARED() {
            @Override
            public RegressionMethod.Factory regressorFactory(double alpha) {
                return new SquaredLoss.Factory(alpha);
            }
        },
        LOGISTIC() {
            @Override
            public RegressionMethod.Factory regressorFactory(double alpha) {
                return new LogisticLoss.Factory(alpha);
            }
        };

        public abstract RegressionMethod.Factory regressorFactory(double alpha);

        public static RegressorType resolve(String name, SearchContext context) {
            if (name.equals("squared")) {
                return SQUARED;
            } else if (name.equals("logistic")) {
                return LOGISTIC;
            }
            throw new SearchParseException(context, "Unknown " + REGRESSOR + " type [" + name + "]");
        }
    }

    @Override
    public String getName() {
        return "sgd";
    }
}
