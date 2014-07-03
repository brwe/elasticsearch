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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.linearregression.InternalRegression;
import org.elasticsearch.search.aggregations.metrics.linearregression.RegressionMethod;
import org.elasticsearch.search.aggregations.metrics.linearregression.RegressionMethodStreams;
import org.elasticsearch.search.aggregations.metrics.linearregression.RegressionReducer;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.List;

import static java.lang.Math.abs;
import static java.lang.Math.pow;

/**
*
*/
public abstract class SgdRegressor implements RegressionMethod {

    protected double alpha;
    protected double t = 1;
    protected double sum = 0;
    protected double progressiveLoss;
    protected double count = 1;
    protected double last = 0;

    protected double N = 0;

    protected final BigArrays bigArrays;
    protected ObjectArray<double[]> scaleBuckets;
    protected ObjectArray<double[]> thetasBuckets;

    public static final RegressionMethodStreams.Stream STREAM = new RegressionMethodStreams.Stream() {
        @Override
        public RegressionReducer readResult(StreamInput in) throws IOException {
            return new StochasticRegressionReducer(in.readDouble());
        }

        @Override
        public String getName() {
            return "stochastic";
        }
    };

    public SgdRegressor(AggregationContext context, long estimatedBucketsCount, double alpha) {
        this.alpha = alpha;
        this.bigArrays = context.bigArrays();
        this.scaleBuckets = bigArrays.newObjectArray(estimatedBucketsCount);
        this.thetasBuckets = bigArrays.newObjectArray(estimatedBucketsCount);
    }

    protected abstract double loss(double[] thetas, double[] xs, double y);

    public void step(double[] xs, double y, long bucketOrd) {

        // Thetas hold the x coefficients
        thetasBuckets = bigArrays.grow(thetasBuckets, bucketOrd + 1);
        double[] thetas = thetasBuckets.get(bucketOrd);
        if (thetas == null) {
            thetas = new double[xs.length];
            thetasBuckets.set(bucketOrd, thetas);
        }

        // s holds the scaling factor for each x
        scaleBuckets = bigArrays.grow(scaleBuckets, bucketOrd + 1);
        double[] s = scaleBuckets.get(bucketOrd);
        if (s == null) {
            s = new double[xs.length];
            scaleBuckets.set(bucketOrd, s);
        }

        // Normalized Gradient Descent: feature scaling
        for (int i = 0; i < xs.length; i++) {
            double absXi = abs(xs[i]);
            if (absXi > s[i]) {
                thetas[i] = (thetas[i] * pow(s[i], 2)) / pow( absXi, 2);
                s[i] = absXi;
            }
        }

        // Gradient calculation
        double error = loss(thetas, xs, y);

        // Learning rate scaling
        double sumScaled = 0;
        for (int i = 0; i < thetas.length; i++) {
            sumScaled += pow(xs[i], 2) / pow(s[i], 2);
        }
        N += sumScaled;

        // Update thetas
        for (int i = 0; i < thetas.length; i++) {
            thetas[i] += alpha                            // learning rate
                          * (t / N) * (1 / pow(s[i],2))   // feature normalization to make step size invariant
                          * (2 * error * xs[i]);          // step in direction of gradient
        }

        /*
        //Progressive Validation debugging output
        sum += error;
        if (count == pow(2, last)) {
            last += 1;
            progressiveLoss = abs(sum / count);
            System.out.print(pow(2, last) + ":  " + thetas[0] + " " + thetas[1] + "  --  " + progressiveLoss + "\n");

            sum = 0;
            count = 0;
        }
        count += 1;
        */

        t += 1;

    }

    public double[] thetas(long bucketOrd) {
        if (bucketOrd >= thetasBuckets.size() || thetasBuckets.get(bucketOrd) == null) {
            return null;    //TODO what to do here?
        }
        return thetasBuckets.get(bucketOrd);
    }

    public double[] emptyResult() {
        return null;    //TODO what to do here?
    }

    public boolean release() throws ElasticsearchException {
        Releasables.close(thetasBuckets, scaleBuckets);
        return true;
    }

    private static class StochasticRegressionReducer implements RegressionReducer {
        double alpha;
        public StochasticRegressionReducer(double alpha) {
            this.alpha = alpha;
        }

        @Override
        public InternalRegression reduce(List<InternalAggregation> aggregations) {
            InternalRegression reduced = null;
            for (InternalAggregation aggregation : aggregations) {
                if (reduced == null) {
                    reduced = (InternalRegression) aggregation;
                } else {
                    double[] thetas = reduced.getThetas();
                    for (int i = 0; i < reduced.getThetas().length; i++) {
                        thetas[i] += ((InternalRegression) aggregation).getThetas()[i];
                    }
                    reduced.setThetas(thetas);
                }
            }
            if (reduced != null) {
                double[] thetas = reduced.getThetas();
                for (int i = 0; i < thetas.length; i++) {
                    thetas[i] /= aggregations.size();
                }
                reduced.setThetas(thetas);
                return reduced;
            }
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(STREAM.getName());
            out.writeDouble(alpha);
        }
    }

    @Override
    public RegressionReducer getReducer() {
        return new StochasticRegressionReducer(alpha);
    }
}
