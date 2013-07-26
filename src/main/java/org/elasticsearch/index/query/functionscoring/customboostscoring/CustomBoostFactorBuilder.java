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

package org.elasticsearch.index.query.functionscoring.customboostscoring;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscoring.ScoreFunctionBuilder;

import java.io.IOException;

/**
 * A query that simply applies the boost factor to another query (multiply it).
 * 
 * 
 */
public class CustomBoostFactorBuilder implements ScoreFunctionBuilder {

    private float boostFactor = -1;

    /**
     * Sets the boost factor for this query.
     */
    public CustomBoostFactorBuilder boostFactor(float boost) {
        this.boostFactor = boost;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (boostFactor != -1) {
            builder.field("boost_factor", boostFactor);
        }
        return builder;
    }

    @Override
    public String getName() {
        return CustomBoostFactorParser.NAMES[0];
    }
}