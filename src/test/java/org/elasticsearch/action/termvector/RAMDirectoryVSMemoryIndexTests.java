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

package org.elasticsearch.action.termvector;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;

public class RAMDirectoryVSMemoryIndexTests extends ElasticsearchIntegrationTest {


    private final String field_name = "body.text";

    @Test
    public void duelMemoryIndexRAMDirectory() throws Exception {
        DirectoryReader ramDir = indexDocInRAMDircetory();

        MemoryIndex memIndex = indexDocInMemoryIndex();
        Terms ramTv = ramDir.getTermVector(0, "body.text");
        Terms memTv = memIndex.createSearcher().getIndexReader().getTermVector(0, "body.text");
        validateResponse(ramTv, memTv);
    }

    protected MemoryIndex indexDocInMemoryIndex() throws IOException {

        MemoryIndex memoryIndex = new MemoryIndex(true);
        Analyzer ana = new StandardAnalyzer(Version.CURRENT.luceneVersion);
        memoryIndex.addField(field_name, "la la", ana);
        memoryIndex.addField(field_name, "foo bar foo bar foo", ana);
        return memoryIndex;
    }

    protected DirectoryReader indexDocInRAMDircetory() throws IOException {

        Directory dir = new RAMDirectory();
        IndexWriterConfig conf = new IndexWriterConfig(Version.CURRENT.luceneVersion, new StandardAnalyzer(Version.CURRENT.luceneVersion));

        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, conf);
        Document d = new Document();

        FieldType type = new FieldType(TextField.TYPE_STORED);

        type.setStoreTermVectorOffsets(true);
        type.setStoreTermVectorPayloads(true);
        type.setStoreTermVectorPositions(true);
        type.setStoreTermVectors(true);
        type.freeze();
        d.add(new Field(field_name, "la la", type));
        d.add(new Field(field_name, "foo bar foo bar foo", type));
        writer.updateDocument(new Term("id", "1"), d);
        writer.commit();

        return DirectoryReader.open(dir);
    }

    protected void validateResponse(Terms ramTerms, Terms memTerms) throws IOException {

        TermsEnum ramTermEnum = ramTerms.iterator(null);
        TermsEnum memTermEnum = memTerms.iterator(null);

        while (ramTermEnum.next() != null) {
            assertNotNull(memTermEnum.next());

            assertThat(ramTermEnum.totalTermFreq(), equalTo(memTermEnum.totalTermFreq()));
            DocsAndPositionsEnum ramDocsPosEnum = ramTermEnum.docsAndPositions(null, null, 0);
            DocsAndPositionsEnum memDocsPosEnum = memTermEnum.docsAndPositions(null, null, 0);


            String currentTerm = ramTermEnum.term().utf8ToString();

            assertThat("Token mismatch for field: " + field_name, currentTerm, equalTo(memTermEnum.term().utf8ToString()));

            ramDocsPosEnum.nextDoc();
            memDocsPosEnum.nextDoc();

            int freq = ramDocsPosEnum.freq();
            assertThat(freq, equalTo(memDocsPosEnum.freq()));
            for (int i = 0; i < freq; i++) {
                String failDesc = " (field:" + field_name + " term:" + currentTerm + ")";
                int memPos = memDocsPosEnum.nextPosition();
                int ramPos = ramDocsPosEnum.nextPosition();
                assertThat("Position test failed" + failDesc, memPos, equalTo(ramPos));
                assertThat("Offset test failed" + failDesc, memDocsPosEnum.startOffset(), equalTo(ramDocsPosEnum.startOffset()));
                assertThat("Offset test failed" + failDesc, memDocsPosEnum.endOffset(), equalTo(ramDocsPosEnum.endOffset()));
                assertThat("Missing payload test failed" + failDesc, ramDocsPosEnum.getPayload(), equalTo(null));

            }
        }

        assertNull("Es returned terms are done but lucene isn't", memTermEnum.next());


    }
}
