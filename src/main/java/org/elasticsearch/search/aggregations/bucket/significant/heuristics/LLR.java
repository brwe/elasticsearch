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


package org.elasticsearch.search.aggregations.bucket.significant.heuristics;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class LLR extends NXYSignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("llr");

    private LLR() {
    }

    public LLR(boolean includeNegatives, boolean backgroundIsSuperset) {
        super(includeNegatives, backgroundIsSuperset);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LLR)) {
            return false;
        }
        return ((LLR) other).includeNegatives == includeNegatives && ((LLR) other).backgroundIsSuperset == backgroundIsSuperset;
    }

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {
            return new LLR(in.readBoolean(), in.readBoolean());
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    /**
     * Calculates log lokelihood ratio as described in
     * http://tdunning.blogspot.de/2008/03/surprise-and-coincidence.html
     *
     * @param subsetFreq   The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetFreq The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        computeNxys(subsetFreq, subsetSize, supersetFreq, supersetSize);
        return MutualInformation.computeScore(frequencies, includeNegatives, backgroundIsSuperset) * frequencies.N * 2.0 * MutualInformation.log2;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        super.writeTo(out);
    }

    public boolean getIncludeNegatives() {
        return includeNegatives;
    }

    public static class LLRParser extends NXYParser {

        @Override
        protected SignificanceHeuristic newHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
            return new LLR(includeNegatives, backgroundIsSuperset);
        }

        @Override
        protected void checkName(String givenName) {
            NAMES_FIELD.match(givenName, ParseField.EMPTY_FLAGS);
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class LLRBuilder extends NXYBuilder {

        private LLRBuilder() {
        }

        public LLRBuilder(boolean includeNegatives, boolean backgroundIsSuperset) {
            super(includeNegatives, backgroundIsSuperset);
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject(STREAM.getName());
            super.build(builder);
            builder.endObject();
        }
    }
}

