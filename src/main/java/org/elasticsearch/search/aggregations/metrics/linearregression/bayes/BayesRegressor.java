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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.linearregression.*;
import org.elasticsearch.search.aggregations.metrics.linearregression.sgd.SgdRegressor;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.List;

import Jama.Matrix;

/**
 *
 */
public class BayesRegressor implements RegressionMethod {


    protected final BigArrays bigArrays;
    protected ObjectArray<Matrix> covMatsInverted;
    protected ObjectArray<double[]> meanBuckets;
    protected double beta;


    public static final RegressionMethodStreams.Stream STREAM = new RegressionMethodStreams.Stream() {
        @Override
        public RegressionReducer readResult(StreamInput in) throws IOException {
            return new BayesRegressionReducer();
        }

        @Override
        public String getName() {
            return "bayes";
        }
    };

    public BayesRegressor(AggregationContext context, long estimatedBucketsCount, double beta) {
        this.bigArrays = context.bigArrays();
        this.covMatsInverted = bigArrays.newObjectArray(estimatedBucketsCount);
        this.meanBuckets = bigArrays.newObjectArray(estimatedBucketsCount);
        this.beta = beta;
    }

    public void step(double[] xs, double y, long bucketOrd) {

        covMatsInverted = bigArrays.grow(covMatsInverted, bucketOrd + 1);
        meanBuckets = bigArrays.grow(meanBuckets, bucketOrd + 1);
        double[] means = meanBuckets.get(bucketOrd);

        Matrix covMatInverted = this.covMatsInverted.get(bucketOrd);
        if (means == null) {
            means = new double[xs.length];
            meanBuckets.set(bucketOrd, means);
            covMatInverted = Matrix.identity(xs.length, xs.length);
            covMatsInverted.set(bucketOrd, covMatInverted);
        }
        Matrix meanMatrix = new Matrix(means, 1);
        Matrix xAsMatrix = new Matrix(xs, 1);

        // new Sn as in Bishop, "Pattern Recognition and Machine Learning", Eq. 3.51
        Matrix xTimesX = xAsMatrix.transpose().times(xAsMatrix).times(beta);
        Matrix newCovMatInverted = covMatInverted.plus(xTimesX);

        // new mn as in Bishop, "Pattern Recognition and Machine Learning", Eq. 3.50
        Matrix s0TimesM0 = covMatInverted.times(meanMatrix.transpose());
        Matrix betaTimesPhiTimesT = xAsMatrix.times(y).times(beta);
        meanMatrix = s0TimesM0.plus(betaTimesPhiTimesT.transpose());
        meanMatrix = meanMatrix.transpose().times(newCovMatInverted.inverse());
        covMatsInverted.set(bucketOrd, newCovMatInverted);
        meanBuckets.set(bucketOrd, meanMatrix.getColumnPackedCopy());
    }

    public double[] thetas(long bucketOrd) {
        if (bucketOrd >= meanBuckets.size() || meanBuckets.get(bucketOrd) == null) {
            return null;    //TODO what to do here?
        }
        return meanBuckets.get(bucketOrd);
    }

    public double[] emptyResult() {
        return null;    //TODO what to do here?
    }

    public boolean release() throws ElasticsearchException {
        Releasables.close(covMatsInverted, meanBuckets);
        return true;
    }

    @Override
    public void close() throws ElasticsearchException {
        release();
    }

    private static class BayesRegressionReducer implements RegressionReducer {
        public BayesRegressionReducer() {
        }

        // TODO: Proper reduce, can be exact
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
        }

        @Override
        public void resultToXContent(XContentBuilder builder) {
            // nothing to build, maybe output error or something later?
        }
    }

    @Override
    public RegressionReducer getReducer() {
        return new BayesRegressionReducer();
    }

    public static class Factory implements SgdRegressor.Factory {

        double beta;
        public Factory(double beta) {
            this.beta = beta;
        }

        public RegressionMethod create(long estimatedBucketCount, AggregationContext context) {
            return new BayesRegressor(context, estimatedBucketCount, beta);
        }
    }

    public static class Builder implements RegressionMethodBuilder {
        double beta = 1.0;
        public Builder(double beta) {
            this.beta = beta;
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject("bayes");
            builder.field("beta", beta);
            builder.endObject();
        }
    }
}
