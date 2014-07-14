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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.search.child.TestSearchContext;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.*;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SignificanceHeuristicTests extends ElasticsearchTestCase {

    static class SignificantTermsTestSearchContext extends TestSearchContext {
        @Override
        public int numberOfShards() {
            return 1;
        }

        @Override
        public SearchShardTarget shardTarget() {
            return new SearchShardTarget("no node, this is a unit test", "no index, this is a unit test", 0);
        }
    }

    // test that stream output can actually be read - does not replace bwc test
    @Test
    public void streamResponse() throws Exception {
        SignificanceHeuristicStreams.registerStream(MutualInformation.STREAM, MutualInformation.STREAM.getName());
        SignificanceHeuristicStreams.registerStream(JLHScore.STREAM, JLHScore.STREAM.getName());
        SignificanceHeuristicStreams.registerStream(GND.STREAM, GND.STREAM.getName());
        Version version = ElasticsearchIntegrationTest.randomVersion();
        InternalSignificantTerms[] sigTerms = getRandomSignificantTerms(getRandomSignificanceheuristic());

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(version);

        sigTerms[0].writeTo(out);

        // read
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(version);

        sigTerms[1].readFrom(in);

        if (version.onOrAfter(Version.V_1_3_0)) {
            assertTrue(sigTerms[1].significanceHeuristic.equals(sigTerms[0].significanceHeuristic));
        } else {
            assertTrue(sigTerms[1].significanceHeuristic instanceof JLHScore);
        }
    }

    InternalSignificantTerms[] getRandomSignificantTerms(SignificanceHeuristic heuristic) {
        InternalSignificantTerms[] sTerms = new InternalSignificantTerms[2];
        ArrayList<InternalSignificantTerms.Bucket> buckets = new ArrayList<>();
        if (randomBoolean()) {
            BytesRef term = new BytesRef("123.0");
            buckets.add(new SignificantLongTerms.Bucket(1, 2, 3, 4, 123, InternalAggregations.EMPTY));
            sTerms[0] = new SignificantLongTerms(10, 20, "some_name", null, 1, 1, heuristic, buckets);
            sTerms[1] = new SignificantLongTerms();
        } else {

            BytesRef term = new BytesRef("someterm");
            buckets.add(new SignificantStringTerms.Bucket(term, 1, 2, 3, 4, InternalAggregations.EMPTY));
            sTerms[0] = new SignificantStringTerms(10, 20, "some_name", 1, 1, heuristic, buckets);
            sTerms[1] = new SignificantStringTerms();
        }
        return sTerms;
    }

    SignificanceHeuristic getRandomSignificanceheuristic() {
        List<SignificanceHeuristic> heuristics = new ArrayList<>();
        heuristics.add(JLHScore.INSTANCE);
        heuristics.add(new MutualInformation(randomBoolean(), true));
        heuristics.add(GND.INSTANCE);
        return heuristics.get(randomInt(2));
    }

    // test that
    // 1. The output of the builders can actually be parsed
    // 2. The parser does not swallow parameters after a significance heuristic was defined
    @Test
    public void testBuilderAndParser() throws Exception {

        Set<SignificanceHeuristicParser> parsers = new HashSet<>();
        parsers.add(new JLHScore.JLHScoreParser());
        parsers.add(new MutualInformation.MutualInformationParser());
        parsers.add(new GND.GNDParser());
        SignificanceHeuristicParserMapper heuristicParserMapper = new SignificanceHeuristicParserMapper(parsers);
        SearchContext searchContext = new SignificantTermsTestSearchContext();

        // test jlh with string
        assertTrue(parseFromString(heuristicParserMapper, searchContext, "\"jlh\":{}") instanceof JLHScore);
        // test gnd with string
        assertTrue(parseFromString(heuristicParserMapper, searchContext, "\"gnd\":{}") instanceof GND);
        // test mutual information with string
        boolean includeNegatives = randomBoolean();
        boolean backgroundIsSuperset = randomBoolean();
        assertThat(parseFromString(heuristicParserMapper, searchContext, "\"mutual_information\":{\"include_negatives\": " + includeNegatives + ", \"background_is_superset\":" + backgroundIsSuperset + "}"), equalTo((SignificanceHeuristic) (new MutualInformation(includeNegatives, backgroundIsSuperset))));

        // test with builders
        assertTrue(parseFromBuilder(heuristicParserMapper, searchContext, new JLHScore.JLHScoreBuilder()) instanceof JLHScore);
        assertTrue(parseFromBuilder(heuristicParserMapper, searchContext, new GND.GNDBuilder()) instanceof GND);
        assertThat(parseFromBuilder(heuristicParserMapper, searchContext, new MutualInformation.MutualInformationBuilder(includeNegatives, backgroundIsSuperset)), equalTo((SignificanceHeuristic) new MutualInformation(includeNegatives, backgroundIsSuperset)));

        // test exceptions
        String faultyHeuristicdefinition = "\"mutual_information\":{\"include_negatives\": false, \"some_unknown_field\": false}";
        String expectedError = "unknown for mutual_information";
        checkParseException(heuristicParserMapper, searchContext, faultyHeuristicdefinition, expectedError);

        faultyHeuristicdefinition = "\"jlh\":{\"unknown_field\": true}";
        expectedError = "expected }, got ";
        checkParseException(heuristicParserMapper, searchContext, faultyHeuristicdefinition, expectedError);

        faultyHeuristicdefinition = "\"gnd\":{\"unknown_field\": true}";
        expectedError = "expected }, got ";
        checkParseException(heuristicParserMapper, searchContext, faultyHeuristicdefinition, expectedError);
    }

    protected void checkParseException(SignificanceHeuristicParserMapper heuristicParserMapper, SearchContext searchContext, String faultyHeuristicDefinition, String expectedError) throws IOException {
        try {
            XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"text\", " + faultyHeuristicDefinition + ",\"min_doc_count\":200}");
            stParser.nextToken();
            new SignificantTermsParser(heuristicParserMapper).parse("testagg", stParser, searchContext);
            fail();
        } catch (ElasticsearchParseException e) {
            assertTrue(e.getMessage().contains(expectedError));
        }
    }

    protected SignificanceHeuristic parseFromBuilder(SignificanceHeuristicParserMapper heuristicParserMapper, SearchContext searchContext, SignificanceHeuristicBuilder significanceHeuristicBuilder) throws IOException {
        SignificantTermsBuilder stBuilder = new SignificantTermsBuilder("testagg");
        stBuilder.significanceHeuristic(significanceHeuristicBuilder).field("text").minDocCount(200);
        XContentBuilder stXContentBuilder = XContentFactory.jsonBuilder();
        stBuilder.internalXContent(stXContentBuilder, null);
        XContentParser stParser = JsonXContent.jsonXContent.createParser(stXContentBuilder.string());
        return parseSignificanceHeuristic(heuristicParserMapper, searchContext, stParser);
    }

    private SignificanceHeuristic parseSignificanceHeuristic(SignificanceHeuristicParserMapper heuristicParserMapper, SearchContext searchContext, XContentParser stParser) throws IOException {
        stParser.nextToken();
        SignificantTermsAggregatorFactory aggregatorFactory = (SignificantTermsAggregatorFactory) new SignificantTermsParser(heuristicParserMapper).parse("testagg", stParser, searchContext);
        stParser.nextToken();
        assertThat(aggregatorFactory.getBucketCountThresholds().getMinDocCount(), equalTo(200l));
        assertThat(stParser.currentToken(), equalTo(null));
        stParser.close();
        return aggregatorFactory.getSignificanceHeuristic();
    }

    protected SignificanceHeuristic parseFromString(SignificanceHeuristicParserMapper heuristicParserMapper, SearchContext searchContext, String heuristicString) throws IOException {
        XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"text\", " + heuristicString + ", \"min_doc_count\":200}");
        return parseSignificanceHeuristic(heuristicParserMapper, searchContext, stParser);
    }

    @Test
    public void testAssertions() throws Exception {
        MutualInformation mutualInformation = new MutualInformation(true, true);
        try {
            mutualInformation.getScore(2, 3, 1, 4);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > supersetFreq"));
        }
        try {
            mutualInformation.getScore(1, 4, 2, 3);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetSize > supersetSize"));
        }
        try {
            mutualInformation.getScore(2, 1, 3, 4);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > subsetSize"));
        }
        try {
            mutualInformation.getScore(1, 2, 4, 3);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("supersetFreq > supersetSize"));
        }
        try {
            mutualInformation.getScore(1, 3, 4, 4);
            fail();
        } catch (ElasticsearchIllegalArgumentException assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("supersetFreq - subsetFreq > supersetSize - subsetSize"));
        }
        try {
            int idx = randomInt(3);
            long[] values = {1, 2, 3, 4};
            values[idx] *= -1;
            mutualInformation.getScore(values[0], values[1], values[2], values[3]);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("Frequencies of subset and superset must be positive"));
        }
        mutualInformation = new MutualInformation(true, false);
        double score = mutualInformation.getScore(2, 3, 1, 4);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));
        score = mutualInformation.getScore(1, 4, 2, 3);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));

        try {
            mutualInformation.getScore(2, 1, 3, 4);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > subsetSize"));
        }
        try {
            mutualInformation.getScore(1, 2, 4, 3);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("supersetFreq > supersetSize"));
        }

        score = mutualInformation.getScore(1, 3, 4, 4);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));

        try {
            int idx = randomInt(3);
            long[] values = {1, 2, 3, 4};
            values[idx] *= -1;
            mutualInformation.getScore(values[0], values[1], values[2], values[3]);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("Frequencies of subset and superset must be positive"));
        }

        SignificanceHeuristic heuristic = randomBoolean() ? JLHScore.INSTANCE : GND.INSTANCE;
        try {
            int idx = randomInt(3);
            long[] values = {1, 2, 3, 4};
            values[idx] *= -1;
            heuristic.getScore(values[0], values[1], values[2], values[3]);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("Frequencies of subset and superset must be positive"));
        }
        try {
            heuristic.getScore(1, 2, 4, 3);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("supersetFreq > supersetSize"));
        }
        try {
            heuristic.getScore(2, 1, 3, 4);
            fail();
        } catch (ElasticsearchIllegalArgumentException illegalArgumentException) {
            assertNotNull(illegalArgumentException.getMessage());
            assertTrue(illegalArgumentException.getMessage().contains("subsetFreq > subsetSize"));
        }
    }

    @Test
    public void basicScoreProperties() {
        SignificanceHeuristic heuristic = randomBoolean() ? JLHScore.INSTANCE : GND.INSTANCE;
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(0, 1, 2, 3), equalTo(0.0));
        double score = 0.0;
        try {
            long a = randomLong();
            long b = randomLong();
            long c = randomLong();
            long d = randomLong();
            score = heuristic.getScore(a, b, c, d);
        } catch (ElasticsearchIllegalArgumentException e) {
        }
        assertThat(score, greaterThanOrEqualTo(0.0));
    }

    @Test
    public void scoreMutual() throws Exception {
        SignificanceHeuristic heuristic = new MutualInformation(true, true);
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(2, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(0, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(2, 2, 4, 4), equalTo(0.0));
        assertThat(heuristic.getScore(1, 2, 2, 4), equalTo(0.0));
        assertThat(heuristic.getScore(3, 6, 9, 18), equalTo(0.0));

        double score = 0.0;
        try {
            long a = randomLong();
            long b = randomLong();
            long c = randomLong();
            long d = randomLong();
            score = heuristic.getScore(a, b, c, d);
        } catch (ElasticsearchIllegalArgumentException e) {
        }
        assertThat(score, lessThanOrEqualTo(1.0));
        assertThat(score, greaterThanOrEqualTo(0.0));
        heuristic = new MutualInformation(false, true);
        assertThat(heuristic.getScore(0, 1, 2, 3), equalTo(-1.0 * Double.MAX_VALUE));

    }
}
