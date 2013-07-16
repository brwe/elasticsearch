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

import org.apache.lucene.search.Explanation;

/**
 * Implement this interface to provide a decay function that is executed on a
 * distance. For example, this could be an exponential drop of, a triangle
 * function or something of the kind. This is used, for example, by
 * {@link GaussDecayFunctionParser}.
 * 
 * */

public interface CustomDecayFunction {
    public double evaluate(double value, double scale);

    public Explanation explainFunction(String distance, double distanceVal, double scale);
    public double processScale(double userGivenScale, double userGivenValue);

}
