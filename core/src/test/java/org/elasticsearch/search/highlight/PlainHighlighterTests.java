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

package org.elasticsearch.search.highlight;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.spatial.geopoint.search.GeoPointInBBoxQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PlainHighlighterTests extends LuceneTestCase {

    public void testHighlightPhrase() throws Exception {
        Query query = new PhraseQuery.Builder()
            .add(new Term("field", "foo"))
            .add(new Term("field", "bar"))
            .build();
        QueryScorer queryScorer = new CustomQueryScorer(query);
        org.apache.lucene.search.highlight.Highlighter highlighter = new org.apache.lucene.search.highlight.Highlighter(queryScorer);
        String[] frags = highlighter.getBestFragments(new MockAnalyzer(random()), "field", "bar foo bar foo", 10);
        assertArrayEquals(new String[]{"bar <B>foo</B> <B>bar</B> foo"}, frags);
    }

    public void testGeoFieldHighlightingUnitForQuery() throws IOException, InvalidTokenOffsetsException {
        Map analysers = new HashMap<>();
        analysers.put("text", new KeywordAnalyzer());
        FieldNameAnalyzer fieldNameAnalyzer = new FieldNameAnalyzer(analysers);
        org.apache.lucene.search.highlight.Highlighter highlighter = new org.apache.lucene.search.highlight.Highlighter(
            new CustomQueryScorer(new GeoPointInBBoxQuery("geo_point", -170.15625, -64.92354174306496
                , 118.47656249999999, 61.10078883158897),
                null, "geo_point", "geo_point"));
        highlighter.getBestFragment(fieldNameAnalyzer.tokenStream("text", "60.12,60.34"), "60.12,60.34");
    }

    public void testGeoFieldHighlightingUnitForText() throws IOException, InvalidTokenOffsetsException {
        Map analysers = new HashMap<>();
        analysers.put("text", new KeywordAnalyzer());
        FieldNameAnalyzer fieldNameAnalyzer = new FieldNameAnalyzer(analysers);
        org.apache.lucene.search.highlight.Highlighter highlighter = new org.apache.lucene.search.highlight.Highlighter(
            new CustomQueryScorer(new GeoPointInBBoxQuery("geo_point", -60.15625, -64.92354174306496
                , 65.47656249999999, 61.10078883158897),
                null, "geo_point", "geo_point"));
        highlighter.getBestFragment(fieldNameAnalyzer.tokenStream("text", "85.12,120.34"), "85.12,120.34");
    }

    public void testGeoFieldHighlightingThatWorks() throws IOException, InvalidTokenOffsetsException {
        Map analysers = new HashMap<>();
        analysers.put("text", new KeywordAnalyzer());
        FieldNameAnalyzer fieldNameAnalyzer = new FieldNameAnalyzer(analysers);
        org.apache.lucene.search.highlight.Highlighter highlighter = new org.apache.lucene.search.highlight.Highlighter(
            new CustomQueryScorer(new GeoPointInBBoxQuery("geo_point", -60.15625, -64.92354174306496
                , 65.47656249999999, 61.10078883158897),
                null, "geo_point", "geo_point"));
        highlighter.getBestFragment(fieldNameAnalyzer.tokenStream("text", "60.12,60.34"), "60.12,60.34");
    }

}
