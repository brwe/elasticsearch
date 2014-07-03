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
package org.elasticsearch.search.aggregations.metrics.linearregression.bayes;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.linearregression.RegressionMethod;
import org.elasticsearch.search.aggregations.metrics.linearregression.RegressionMethodParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class BayesParser implements RegressionMethodParser {

    public static final String BETA = "beta";
    @Override
    public String getName() {
        return "bayes";
    }

    public RegressionMethod.Factory parse(XContentParser parser, SearchContext context) throws IOException {

        double beta = 0.5;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            if(currentFieldName.equals(BETA)) {
                parser.nextToken();
                beta = parser.doubleValue();
            }
        }
        return new BayesRegressor.Factory(beta);
    }
}
