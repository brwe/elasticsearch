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


public class NormalizedCorrelation {

    public static double normalizedCorrelation(double[] a, double[] b) {
        assert a.length == b.length;
        normalize(a);
        normalize(b);
        double corr = 0.0;
        for (int i = 0; i < a.length; i++) {
            corr += a[i] * b[i];
        }
        return corr / a.length;

    }

    private static void normalize(double[] a) {
        double mean = getMean(a);
        double std = getStdDev(a);
        for (int i = 0; i < a.length; i++) {
            a[i] = (a[i] - mean) / std;
        }
    }

    private static double getMean(double[] array) {
        double sum = 0.0;
        for (double a : array)
            sum += a;
        return sum / array.length;
    }

    private static double getVariance(double[] array) {
        double mean = getMean(array);
        double temp = 0;
        for (double a : array)
            temp += (mean - a) * (mean - a);
        return temp / array.length;
    }

    private static double getStdDev(double[] array) {
        return Math.sqrt(getVariance(array));
    }

    public static double normalizedCorrelation(Object[] values, Object[] referenceValues) {
        return normalizedCorrelation(convertToDoubleArray(values), convertToDoubleArray(referenceValues));
    }

    private static double[] convertToDoubleArray(Object[] values) {
        double[] doubleValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            Object o = values[i];
            doubleValues[i] = ((Number) o).doubleValue();
        }
        return doubleValues;
    }
}
