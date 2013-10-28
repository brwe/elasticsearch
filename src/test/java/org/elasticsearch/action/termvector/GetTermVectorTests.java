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

package org.elasticsearch.action.termvector;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorRequestBuilder;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;

public class GetTermVectorTests extends AbstractTermVectorTests {



    @Test
    public void testNoSuchDoc() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("field")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                .endObject()
                .endObject().endObject();
        ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("type1", mapping));

        ensureYellow();

        client().prepareIndex("test", "type1", "666").setSource("field", "foo bar").execute().actionGet();
        refresh();
        for (int i = 0; i < 20; i++) {
            ActionFuture<TermVectorResponse> termVector = client().termVector(new TermVectorRequest("test", "type1", "" + i));
            TermVectorResponse actionGet = termVector.actionGet();
            assertThat(actionGet, Matchers.notNullValue());
            assertThat(actionGet.isExists(), Matchers.equalTo(false));

        }

    }
    
    @Test
    public void testExistingFieldWithNoTermVectorsNoNPE() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("existingfield")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                .endObject()
                .endObject().endObject();
        ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("type1", mapping));

        ensureYellow();
        // when indexing a field that simply has a question mark, the term
        // vectors will be null
        client().prepareIndex("test", "type1", "0").setSource("existingfield", "?").execute().actionGet();
        refresh();
        String[] selectedFields = { "existingfield" };
        ActionFuture<TermVectorResponse> termVector = client().termVector(
                new TermVectorRequest("test", "type1", "0").selectedFields(selectedFields));
        // lets see if the null term vectors are caught...
        termVector.actionGet();
        TermVectorResponse actionGet = termVector.actionGet();
        assertThat(actionGet.isExists(), Matchers.equalTo(true));

    }
    
    @Test
    public void testExistingFieldButNotInDocNPE() throws Exception {

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("existingfield")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                .endObject()
                .endObject().endObject();
        ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureYellow();
        // when indexing a field that simply has a question mark, the term
        // vectors will be null
        client().prepareIndex("test", "type1", "0").setSource("anotherexistingfield", 1).execute().actionGet();
        refresh();
        String[] selectedFields = { "existingfield" };
        ActionFuture<TermVectorResponse> termVector = client().termVector(
                new TermVectorRequest("test", "type1", "0").selectedFields(selectedFields));
        // lets see if the null term vectors are caught...
        TermVectorResponse actionGet = termVector.actionGet();
        assertThat(actionGet.isExists(), Matchers.equalTo(true));

    }
    


    @Test
    public void testSimpleTermVectors() throws ElasticSearchException, IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("field")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                            .field("analyzer", "tv_test")
                        .endObject()
                .endObject()
                .endObject().endObject();
        ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("type1", mapping)
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")));
        ensureYellow();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog")
                            // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                            // 31the34 35lazy39 40dog43
                            .endObject()).execute().actionGet();
            refresh();
        }
        String[] values = {"brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the"};
        int[] freq = {1, 1, 1, 1, 1, 1, 1, 2};
        int[][] pos = {{2}, {8}, {3}, {4}, {7}, {5}, {1}, {0, 6}};
        int[][] startOffset = {{10}, {40}, {16}, {20}, {35}, {26}, {4}, {0, 31}};
        int[][] endOffset = {{15}, {43}, {19}, {25}, {39}, {30}, {9}, {3, 34}};
        for (int i = 0; i < 10; i++) {
            TermVectorRequestBuilder resp = client().prepareTermVector("test", "type1", Integer.toString(i)).setPayloads(true)
                    .setOffsets(true).setPositions(true).setSelectedFields();
            TermVectorResponse response = resp.execute().actionGet();
            assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(1));
            Terms terms = fields.terms("field");
            assertThat(terms.size(), equalTo(8l));
            TermsEnum iterator = terms.iterator(null);
            for (int j = 0; j < values.length; j++) {
                String string = values[j];
                BytesRef next = iterator.next();
                assertThat(next, Matchers.notNullValue());
                assertThat("expected " + string, string, equalTo(next.utf8ToString()));
                assertThat(next, Matchers.notNullValue());
                // do not test ttf or doc frequency, because here we have many
                // shards and do not know how documents are distributed
                DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
                assertThat(docsAndPositions.nextDoc(), equalTo(0));
                assertThat(freq[j], equalTo(docsAndPositions.freq()));
                int[] termPos = pos[j];
                int[] termStartOffset = startOffset[j];
                int[] termEndOffset = endOffset[j];
                assertThat(termPos.length, equalTo(freq[j]));
                assertThat(termStartOffset.length, equalTo(freq[j]));
                assertThat(termEndOffset.length, equalTo(freq[j]));
                for (int k = 0; k < freq[j]; k++) {
                    int nextPosition = docsAndPositions.nextPosition();
                    assertThat("term: " + string, nextPosition, equalTo(termPos[k]));
                    assertThat("term: " + string, docsAndPositions.startOffset(), equalTo(termStartOffset[k]));
                    assertThat("term: " + string, docsAndPositions.endOffset(), equalTo(termEndOffset[k]));
                    assertThat("term: " + string, docsAndPositions.getPayload(), equalTo(new BytesRef("word")));
                }
            }
            assertThat(iterator.next(), Matchers.nullValue());
        }
    }
    
    public class TermInfo {
        int[] positions;
        int[] startOffsets;
        int[] endOffsets;
        int freq;
        public TermInfo(int freq,  int[] positions, int[] startOffsets, int[] endOffsets) {
            this.freq = freq;
            this.positions = positions;
            this.startOffsets = startOffsets;
            this.endOffsets = endOffsets;
        }
    }

    @Test
    public void testRandomSingleTermVectors() throws ElasticSearchException, IOException {
        FieldType ft = createFieldType(randomInt(6));
        

        String optionString = AbstractFieldMapper.termVectorOptionsToString(ft);
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("field")
                            .field("type", "string")
                            .field("term_vector", optionString)
                            .field("analyzer", "tv_test")
                        .endObject()
                .endObject()
                .endObject().endObject();
        ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("type1", mapping)
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")));
        ensureYellow();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog")
                            // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                            // 31the34 35lazy39 40dog43
                            .endObject()).execute().actionGet();
            refresh();
        }
        String[] values = {"brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the"};
        int[] freqencies = {1, 1, 1, 1, 1, 1, 1, 2};
        int[][] pos = {{2}, {8}, {3}, {4}, {7}, {5}, {1}, {0, 6}};
        int[][] startOffset = {{10}, {40}, {16}, {20}, {35}, {26}, {4}, {0, 31}};
        int[][] endOffset = {{15}, {43}, {19}, {25}, {39}, {30}, {9}, {3, 34}};
        String[] missingTerms = {"anotindoc1", "notindoc2", "znotindoc3"};
        Map<String, TermInfo> termInfos = new HashMap<String, TermInfo>();
        for (int i = 0; i < values.length; i++) {
            termInfos.put(values[i], new TermInfo(freqencies[i], pos[i], startOffset[i], endOffset[i]));
        }
        for (int i = 0; i < missingTerms.length; i++) {
            termInfos.put(missingTerms[i], new TermInfo(0, null, null, null));
        }
        boolean isPayloadRequested = randomBoolean();
        boolean isOffsetRequested = randomBoolean();
        boolean isPositionsRequested = randomBoolean();
        String infoString = createInfoString(isPositionsRequested, isOffsetRequested, isPayloadRequested, optionString);
        boolean getOnlySelectedTerms = randomBoolean();
       
        //first, we create the list of selected terms randomly. they can contain duplicates and the list is unsorted
        List<String> selectedTerms = null; 
        if (getOnlySelectedTerms) {
            selectedTerms = getRandomSelectedTerms(values, missingTerms);
        }
        //now we create the sorted list of terms without duplicates. this is the terms that we expect to be returned
        String[] finalSortedTerms = getSortedExpectedTermsList(values, selectedTerms);
        
        for (int i = 0; i < 10; i++) {
            TermVectorRequestBuilder requestBuilder = client().prepareTermVector("test", "type1", Integer.toString(i))
                    .setPayloads(isPayloadRequested).setOffsets(isOffsetRequested).setPositions(isPositionsRequested);
            if (getOnlySelectedTerms) {
                requestBuilder.setSelectedTerms(selectedTerms.toArray(new String[selectedTerms.size()]));
            }
           
            TermVectorResponse response = requestBuilder.execute().actionGet();
            assertThat(infoString + "doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(ft.storeTermVectors() ? 1 : 0));
            if (ft.storeTermVectors()) {
                Terms terms = fields.terms("field");
                assertThat(terms.size(), equalTo((long) finalSortedTerms.length));
                TermsEnum iterator = terms.iterator(null);
                for (int j = 0; j < finalSortedTerms.length; j++) {
                    BytesRef next = iterator.next();
                    String currTerm = finalSortedTerms[j];
                    assertThat(currTerm, equalTo(next.utf8ToString()));
                    assertThat(infoString, next, Matchers.notNullValue());

                    
                    DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
                    assertThat(infoString, docsAndPositions.nextDoc(), equalTo(0));
                    int curFreq = termInfos.get(currTerm).freq;
                    assertThat(infoString, curFreq, equalTo(docsAndPositions.freq()));
                    assertThat(infoString + "expected " + currTerm, currTerm, equalTo(next.utf8ToString()));
                    
                    if (curFreq == 0) {
                        //the term was a selected term that did not appear in the field, so there is nothing more to check 
                        continue;
                    }
                    int[] termPos = termInfos.get(currTerm).positions;
                    int[] termStartOffset = termInfos.get(currTerm).startOffsets;
                    int[] termEndOffset = termInfos.get(currTerm).endOffsets;
                    
                    if (isPositionsRequested && ft.storeTermVectorPositions()) {
                        assertThat(infoString, termPos.length, equalTo(curFreq));
                    }
                    if (isOffsetRequested && ft.storeTermVectorOffsets()) {
                        assertThat(termStartOffset.length, equalTo(curFreq));
                        assertThat(termEndOffset.length, equalTo(curFreq));
                    }
                    // do not test ttf or doc frequency, because here we have
                    // many shards and do not know how documents are distributed
                    
                    //now, check if positions offsets and payloads are ok
                    for (int k = 0; k < curFreq; k++) {
                        int nextPosition = docsAndPositions.nextPosition();
                        // only return something useful if requested and stored
                        if (isPositionsRequested && ft.storeTermVectorPositions()) {
                            assertThat(infoString + "positions for term: " + currTerm, nextPosition, equalTo(termPos[k]));
                        } else {
                            assertThat(infoString + "positions for term: ", nextPosition, equalTo(-1));
                        }
                        // only return something useful if requested and stored
                        if (isPayloadRequested && ft.storeTermVectorPayloads()) {
                            assertThat(infoString + "payloads for term: " + currTerm, docsAndPositions.getPayload(), equalTo(new BytesRef(
                                    "word")));
                        } else {
                            assertThat(infoString + "payloads for term: " + currTerm, docsAndPositions.getPayload(), equalTo(null));
                        }
                        // only return something useful if requested and stored
                        if (isOffsetRequested && ft.storeTermVectorOffsets()) {

                            assertThat(infoString + "startOffsets term: " + currTerm, docsAndPositions.startOffset(),
                                    equalTo(termStartOffset[k]));
                            assertThat(infoString + "endOffsets term: " + currTerm, docsAndPositions.endOffset(), equalTo(termEndOffset[k]));
                        } else {
                            assertThat(infoString + "startOffsets term: " + currTerm, docsAndPositions.startOffset(), equalTo(-1));
                            assertThat(infoString + "endOffsets term: " + currTerm, docsAndPositions.endOffset(), equalTo(-1));
                        }

                    }
                }
                assertThat(iterator.next(), Matchers.nullValue());
            }

        }
    }

    private FieldType createFieldType(int randomInt) {
        FieldType ft = new FieldType();
        int config = randomInt(6);
        boolean storePositions = false;
        boolean storeOffsets = false;
        boolean storePayloads = false;
        boolean storeTermVectors = false;
        switch (config) {
            case 0: {
                // do nothing
            }
            case 1: {
                storeTermVectors = true;
            }
            case 2: {
                storeTermVectors = true;
                storePositions = true;
            }
            case 3: {
                storeTermVectors = true;
                storeOffsets = true;
            }
            case 4: {
                storeTermVectors = true;
                storePositions = true;
                storeOffsets = true;
            }
            case 5: {
                storeTermVectors = true;
                storePositions = true;
                storePayloads = true;
            }
            case 6: {
                storeTermVectors = true;
                storePositions = true;
                storeOffsets = true;
                storePayloads = true;
            }
        }
        ft.setStoreTermVectors(storeTermVectors);
        ft.setStoreTermVectorOffsets(storeOffsets);
        ft.setStoreTermVectorPayloads(storePayloads);
        ft.setStoreTermVectorPositions(storePositions);
        return ft;
    }

    private String[] getSortedExpectedTermsList(String[] values, List<String> selectedTerms) {
        Set<String> finalTerms = null;
        if (selectedTerms != null) {
            finalTerms = new HashSet<String>();
            finalTerms.addAll(selectedTerms);
            if (finalTerms.size() == 0) {
                //we expect all terms to be returned if no selected terms are given, so we add all terms here to the expected list
                finalTerms.addAll(Sets.newHashSet(values));
            }
        } else {
            finalTerms = Sets.newHashSet(values);
        }
        String[] finalSortedTerms = new String[finalTerms.size()];
        List<String> termsList = Lists.newArrayList(finalTerms.toArray(finalSortedTerms));
        Collections.sort(termsList);
        finalSortedTerms = termsList.toArray(finalSortedTerms);
        return finalSortedTerms;
    }

    private List<String> getRandomSelectedTerms(String[] values, String[] missingTerms) {
        
        int numTerms = randomInt(values.length);
        int numTermsMissing = randomInt(missingTerms.length);
        List<String> allTerms = new ArrayList<String>();
        for (int i = 0; i < numTerms; i++) {
            int termPos = randomInt(values.length - 1);
            allTerms.add(values[termPos]);
        }
        for (int i = 0; i < numTermsMissing; i++) {
            int termPos = randomInt(missingTerms.length - 1);
            allTerms.add(missingTerms[termPos]);
        }
        return allTerms;
    }

    private String createInfoString(boolean isPositionsRequested, boolean isOffsetRequested, boolean isPayloadRequested,
                                    String optionString) {
        String ret = "Store config: " + optionString + "\n" + "Requested: pos-"
                + (isPositionsRequested ? "yes" : "no") + ", offsets-" + (isOffsetRequested ? "yes" : "no") + ", payload- "
                + (isPayloadRequested ? "yes" : "no") + "\n";
        return ret;
    }

    @Test
    public void testDuelESLucene() throws Exception {
        TestFieldSetting[] testFieldSettings = getFieldSettings();
        createIndexBasedOnFieldSettings(testFieldSettings, -1);
        TestDoc[] testDocs = generateTestDocs(5, testFieldSettings);

        DirectoryReader directoryReader = indexDocsWithLucene(testDocs);
        TestConfig[] testConfigs = generateTestConfigs(20, testDocs, testFieldSettings);

        for (TestConfig test : testConfigs) {
            try {
                TermVectorRequestBuilder request = getRequestForConfig(test);
                if (test.expectedException != null) {
                    assertThrows(request, test.expectedException);
                    continue;
                }

                TermVectorResponse response = run(request);
                Fields luceneTermVectors = getTermVectorsFromLucene(directoryReader, test.doc);
                validateResponse(response, luceneTermVectors, test);
            } catch (Throwable t) {
                throw new Exception("Test exception while running " + test.toString(), t);
            }
        }
    }

    @Test
    public void testRandomPayloadWithDelimitedPayloadTokenFilter() throws ElasticSearchException, IOException {
        
        //create the test document
        int encoding = randomIntBetween(0, 2);
        String encodingString = "";
        if (encoding == 0) {
            encodingString = "float";
        }
        if (encoding == 1) {
            encodingString = "int";
        }
        if (encoding == 2) {
            encodingString = "identity";
        }
        String[] tokens = crateRandomTokens();
        Map<String, List<BytesRef>> payloads = createPayloads(tokens, encoding);
        String delimiter = createRandomDelimiter(tokens);
        String queryString = createString(tokens, payloads, encoding, delimiter.charAt(0));
        //create the mapping
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("field").field("type", "string").field("term_vector", "with_positions_offsets_payloads")
                .field("analyzer", "payload_test").endObject().endObject().endObject().endObject();
        ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("type1", mapping).setSettings(
                ImmutableSettings.settingsBuilder().put("index.analysis.analyzer.payload_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.payload_test.filter", "my_delimited_payload_filter")
                        .put("index.analysis.filter.my_delimited_payload_filter.delimiter", delimiter)
                        .put("index.analysis.filter.my_delimited_payload_filter.encoding", encodingString)
                        .put("index.analysis.filter.my_delimited_payload_filter.type", "delimited_payload_filter")));
        ensureYellow();

        client().prepareIndex("test", "type1", Integer.toString(1))
                .setSource(XContentFactory.jsonBuilder().startObject().field("field", queryString).endObject()).execute().actionGet();
        refresh();
        TermVectorRequestBuilder resp = client().prepareTermVector("test", "type1", Integer.toString(1)).setPayloads(true).setOffsets(true)
                .setPositions(true).setSelectedFields();
        TermVectorResponse response = resp.execute().actionGet();
        assertThat("doc id 1 doesn't exists but should", response.isExists(), equalTo(true));
        Fields fields = response.getFields();
        assertThat(fields.size(), equalTo(1));
        Terms terms = fields.terms("field");
        TermsEnum iterator = terms.iterator(null);
        while (iterator.next() != null) {
            String term = iterator.term().utf8ToString();
            DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
            assertThat(docsAndPositions.nextDoc(), equalTo(0));
            List<BytesRef> curPayloads = payloads.get(term);
            assertThat(term, curPayloads, Matchers.notNullValue());
            assert docsAndPositions != null;
            for (int k = 0; k < docsAndPositions.freq(); k++) {
                docsAndPositions.nextPosition();
                if (docsAndPositions.getPayload()!=null){
                    String infoString = "\nterm: " + term + " has payload \n"+ docsAndPositions.getPayload().toString() + "\n but should have payload \n"+curPayloads.get(k).toString();
                    assertThat(infoString, docsAndPositions.getPayload(), equalTo(curPayloads.get(k)));
                } else {
                    String infoString = "\nterm: " + term + " has no payload but should have payload \n"+curPayloads.get(k).toString();
                    assertThat(infoString, curPayloads.get(k).length, equalTo(0));
                }
            }
        }
        assertThat(iterator.next(), Matchers.nullValue());
    }
    private String createRandomDelimiter(String[] tokens) {
        String delimiter = "";
        boolean isTokenOrWhitespace = true;
        while(isTokenOrWhitespace) {
            isTokenOrWhitespace = false;
            delimiter = randomUnicodeOfLength(1);
            for(String token:tokens) {
                if(token.contains(delimiter)) {
                    isTokenOrWhitespace = true;
                }
            }
            if(Character.isWhitespace(delimiter.charAt(0))) {
                isTokenOrWhitespace = true;
            }
        }
        return delimiter;
    }
    private String createString(String[] tokens, Map<String, List<BytesRef>> payloads, int encoding, char delimiter) {
        String resultString = "";
        ObjectIntOpenHashMap<String> payloadCounter = new ObjectIntOpenHashMap<String>();
        for (String token : tokens) {
            if (!payloadCounter.containsKey(token)) {
                payloadCounter.putIfAbsent(token, 0);
            } else {
                payloadCounter.put(token, payloadCounter.get(token) + 1);
            }
            resultString = resultString + token;
            BytesRef payload = payloads.get(token).get(payloadCounter.get(token));
            if (payload.length > 0) {
                resultString = resultString + delimiter;
                switch (encoding) {
                case 0: {
                    resultString = resultString + Float.toString(PayloadHelper.decodeFloat(payload.bytes, payload.offset));
                    break;
                }
                case 1: {
                    resultString = resultString + Integer.toString(PayloadHelper.decodeInt(payload.bytes, payload.offset));
                    break;
                }
                case 2: {
                    resultString = resultString + payload.utf8ToString();
                    break;
                }
                default: {
                    throw new ElasticSearchException("unsupported encoding type");
                }
                }
            }
            resultString = resultString + " ";
        }
        return resultString;
    }

    private Map<String, List<BytesRef>> createPayloads(String[] tokens, int encoding) {
        Map<String, List<BytesRef>> payloads = new HashMap<String, List<BytesRef>>();
        for (String token : tokens) {
            if (payloads.get(token) == null) {
                payloads.put(token, new ArrayList<BytesRef>());
            }
            boolean createPayload = randomBoolean();
            if (createPayload) {
                switch (encoding) {
                case 0: {
                    float theFloat = randomFloat();
                    payloads.get(token).add(new BytesRef(PayloadHelper.encodeFloat(theFloat)));
                    break;
                }
                case 1: {
                    payloads.get(token).add(new BytesRef(PayloadHelper.encodeInt(randomInt())));
                    break;
                }
                case 2: {
                    String payload = randomUnicodeOfLengthBetween(50, 100);
                    for (int c = 0; c < payload.length(); c++) {
                        if (Character.isWhitespace(payload.charAt(c))) {
                            payload = payload.replace(payload.charAt(c), 'w');
                        }
                    }
                    payloads.get(token).add(new BytesRef(payload));
                    break;
                }
                default: {
                    throw new ElasticSearchException("unsupported encoding type");
                }
                }
            } else {
                payloads.get(token).add(new BytesRef());
            }
        }
        return payloads;
    }

    private String[] crateRandomTokens() {
        String[] tokens = { "the", "quick", "brown", "fox" };
        int numTokensWithDuplicates = randomIntBetween(3, 15);
        String[] finalTokens = new String[numTokensWithDuplicates];
        for (int i = 0; i < numTokensWithDuplicates; i++) {
            finalTokens[i] = tokens[randomIntBetween(0, tokens.length - 1)];
        }
        return finalTokens;
    }
}
