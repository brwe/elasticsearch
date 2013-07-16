/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.distancescoring.multiplydistancescores;

import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;

public class ExponentialDecayFunctionParser extends MultiplyingFunctionParser {

    public static String NAME = "exp";

    @Override
    public String getName() {
        return NAME;
    }

    static CustomDecayFunction distanceFunction = new ExponentialDecayScoreFunction();

    @Override
    public CustomDecayFunction getDecayFunction() {
        return distanceFunction;
    }

    final static class ExponentialDecayScoreFunction implements CustomDecayFunction {

        @Override
        public double evaluate(double value, double scale) {
            return Math.exp(-scale*Math.abs(value));
        }

        @Override
        public Explanation explainFunction(String distance, double distanceVal, double scale) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setValue((float) evaluate(distanceVal, scale));
            ce.setDescription("exp(- abs(" + distance + ")/" + scale + ")");
            return ce;
        }

        /**
         * 
         * */
        @Override
        public double processScale(double userGivenScale, double userGivenValue) {
            return -Math.log(userGivenValue)/userGivenScale;
        }

    }
}
