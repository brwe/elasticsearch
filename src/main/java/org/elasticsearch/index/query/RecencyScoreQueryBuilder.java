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

package org.elasticsearch.index.query;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A query that uses a script to compute the score.
 * 
 * 
 */
public class RecencyScoreQueryBuilder extends BaseQueryBuilder {

    private final QueryBuilder queryBuilder;

    Long timePoint = null; // user supplied point of reference or current time
                           // if no time was given
    String timeOfDoc = null; // field name, time point in the document used for
                             // scoring
    String function = null;

    private Map<String, Object> params = null;

    /**
     * A query that multiplies boost factor from another query with a function
     * centered about a user given time point. The farther the time point of the
     * document is from the user given time, the lower the score is weighted.
     * 
     * @param queryBuilder
     *            The query to apply the boost factor to.
     */
    public RecencyScoreQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * Sets field name of the time point in the document (such as "birthday" or
     * "release-date")
     */
    public RecencyScoreQueryBuilder field(String timeOfDoc) {
        this.timeOfDoc = timeOfDoc;
        return this;
    }

    /**
     * Sets the reference time point given by the user.
     */
    public RecencyScoreQueryBuilder now(long timePoint) {
        this.timePoint = timePoint;
        return this;
    }

    /**
     * Additional parameters such as the parameters for the scoring function.
     * Currently this is only the std of the gaussian used for scoring.
     */
    public RecencyScoreQueryBuilder params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return this;
    }

    /**
     * Additional parameters that can be provided. //TODO: This comes from
     * CustomScoring, what was it good for? Do we need that?
     */
    public RecencyScoreQueryBuilder param(String key, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(key, value);
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RecencyScoreQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        if (timePoint != null) {
            builder.field("now", timePoint);
        }
        builder.field("field", timeOfDoc);
        builder.field("function", function);

        builder.endObject();
    }

    public QueryBuilder function(String string) {
        this.function = string;
        return this;
    }
}