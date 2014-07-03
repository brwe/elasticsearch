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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.linearregression.sgd.SquaredRegressionParser;

import java.io.IOException;

/**
 *
 */
public class LinearRegressionBuilder extends ValuesSourceMetricsAggregationBuilder<LinearRegressionBuilder> {

    String y = null;
    String[] xs = null;
    float[] predict = null;
    boolean display_thetas = false;
    RegressionMethodBuilder methodBuilder;

    public LinearRegressionBuilder setRegressor(RegressionMethodBuilder methodBuilder) {
        this.methodBuilder = methodBuilder;
        return this;
    }

    public LinearRegressionBuilder setY(String y) {
        this.y = y;
        return this;
    }

    public LinearRegressionBuilder setXs(String... xs) {
        this.xs = xs;
        return this;
    }

    public LinearRegressionBuilder setPredict(float... predict) {
        this.predict = predict;
        return this;
    }

    public LinearRegressionBuilder setDisplay_thetas(boolean display_thetas) {
        this.display_thetas = display_thetas;
        return this;
    }

    public LinearRegressionBuilder(String name) {
        super(name, InternalRegression.TYPE.name());
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {

        if (y != null) {
            builder.field(LinearRegressionParser.Y, y);
        }
        if (xs != null) {
            builder.field(LinearRegressionParser.XS, xs);
        }
        if (predict != null) {
            builder.field(LinearRegressionParser.PREDICT, predict);
        }
        if (display_thetas == true) {
            builder.field(LinearRegressionParser.DISPLAY_THETAS, display_thetas);
        }
        methodBuilder.toXContent(builder);
    }
}
