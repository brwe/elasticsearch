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
package org.elasticsearch.search.aggregations.metrics.sgd;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class SgdBuilder extends ValuesSourceMetricsAggregationBuilder<SgdBuilder> {

    String regressor = null;
    String y = null;
    String[] xs = null;
    float[] predict = null;
    boolean display_thetas = false;

    public SgdBuilder setRegressor(String regressor) {
        this.regressor = regressor;
        return this;
    }

    public SgdBuilder setY(String y) {
        this.y = y;
        return this;
    }

    public SgdBuilder setXs(String... xs) {
        this.xs = xs;
        return this;
    }

    public SgdBuilder setPredict(float... predict) {
        this.predict = predict;
        return this;
    }

    public SgdBuilder setDisplay_thetas(boolean display_thetas) {
        this.display_thetas = display_thetas;
        return this;
    }

    public SgdBuilder(String name) {
        super(name, InternalSgd.TYPE.name());
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (regressor != null) {
            builder.field(SgdParser.REGRESSOR, regressor);
        }
        if (y != null) {
            builder.field(SgdParser.Y, y);
        }
        if (xs != null) {
            builder.field(SgdParser.XS, xs);
        }
        if (predict != null) {
            builder.field(SgdParser.PREDICT, predict);
        }
        if (display_thetas == true) {
            builder.field(SgdParser.DISPLAY_THETAS, display_thetas);
        }
    }
}
