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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;

import java.io.IOException;
import java.util.List;

public class InternalLinearRegression extends InternalBucketReducerAggregation implements LinearRegression {

    public static final Type TYPE = new Type("linear_regression");

    double[] parameters;

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalLinearRegression readResult(StreamInput in) throws IOException {
            InternalLinearRegression selections = new InternalLinearRegression();
            selections.readFrom(in);
            return selections;
        }
    };

    public double[] getParameters() {
        return parameters;
    }

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public InternalLinearRegression() {
        super();
    }

    public InternalLinearRegression(String name, List<InternalSelection> selections, double[] parameters) {
        super(name, selections);
        this.parameters = parameters;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        parameters = in.readDoubleArray();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeDoubleArray(parameters);

    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);
        builder.field("estimated_parameters", parameters);
        return builder;
    }
}
