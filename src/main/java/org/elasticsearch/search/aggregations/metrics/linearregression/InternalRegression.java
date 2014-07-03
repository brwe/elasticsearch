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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;

/**
*
*/
public class InternalRegression extends InternalNumericMetricsAggregation.SingleValue implements RegressionResult {

    public final static Type TYPE = new Type("linearregression");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalRegression readResult(StreamInput in) throws IOException {
            InternalRegression result = new InternalRegression();
            result.readFrom(in);
            return result;
        }
    };
    private RegressionReducer regressionReducer;

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double[] thetas;
    private double[] predictXs;
    private boolean displayThetas;

    InternalRegression() {} // for serialization

    InternalRegression(String name, RegressionReducer regressionReducer, double[] thetas, double[] predictXs, boolean displayThetas) {
        super(name);
        this.thetas = thetas;
        this.predictXs = predictXs;
        this.displayThetas = displayThetas;
        this.regressionReducer = regressionReducer;
    }

    @Override
    public double value() {
        double y = thetas[0];
        for (int i = 1; i < thetas.length; i++) {
            y += thetas[i] * predictXs[i-1];
        }
        return y;
    }

    public double getValue() {
        return value();
    }

    @Override
    public Type type() {
        return TYPE;
    }

    public double[] getThetas() {
        return thetas;
    }

    @Override
    public InternalRegression reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return (InternalRegression) aggregations.get(0);
        }
        InternalRegression reduced = regressionReducer.reduce(aggregations);
        if (reduced == null) {
            return (InternalRegression) aggregations.get(0);
        } else {
            return reduced;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        thetas = in.readDoubleArray();
        regressionReducer = RegressionMethodStreams.read(in);

        //TODO: Here also stream the method, must know how to reduce!
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDoubleArray(thetas);
        regressionReducer.writeTo(out);

        //TODO: Here also stream the method, must know how to reduce!
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        double value = value();
        builder.startObject(name);
        builder.field(CommonFields.VALUE, value);
        if (valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(value));
        }
        if (displayThetas) {
            builder.startArray("thetas");
            for (double theta : thetas) {
                builder.value(theta);
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    public void setThetas(double[] thetas) {
        this.thetas = thetas;
    }
}
