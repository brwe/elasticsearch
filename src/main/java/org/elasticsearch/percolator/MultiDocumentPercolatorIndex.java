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


package org.elasticsearch.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of {@link PercolatorIndex} that can hold multiple Lucene documents by
 * opening multiple {@link MemoryIndex} based IndexReaders and wrapping them via a single top level reader.
 */
class MultiDocumentPercolatorIndex implements PercolatorIndex {


    MultiDocumentPercolatorIndex() {
    }

    @Override
    public void prepare(PercolateContext context, List<ParsedDocument> parsedDocumentList) {

        List<ParseContext.Document> allDocs = new ArrayList<ParseContext.Document>();
        List<Analyzer> allAnalyzers = new ArrayList<Analyzer>();
        for (ParsedDocument parsedDocument : parsedDocumentList) {
            List<ParseContext.Document> docs = parsedDocument.docs();
            for (int i = 0; i < docs.size(); i++) {
                ParseContext.Document d = docs.get(i);
                allDocs.add(d);
                allAnalyzers.add(parsedDocument.analyzer());

            }
        }
        context.initialize(addDocuments(allDocs, allAnalyzers), parsedDocumentList);
    }

    public DocSearcher addDocuments(List<ParseContext.Document> docs, List<Analyzer> analyzers) {
        List<IndexReader> memoryIndices = new ArrayList<IndexReader>();
        for (int i=0; i<docs.size(); i++) {
            MemoryIndex memoryIndex = new MemoryIndex(true);
            memoryIndices.add(indexDoc(docs.get(i), analyzers.get(i), memoryIndex).createSearcher().getIndexReader());
        }
        MultiReader mReader = new MultiReader(memoryIndices.toArray(new IndexReader[memoryIndices.size()]), true);
        try {
            AtomicReader slowReader = SlowCompositeReaderWrapper.wrap(mReader);
            return new DocSearcher(new IndexSearcher(slowReader));
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create index for percolator with more than one document ", e);
        }
    }

    MemoryIndex indexDoc(ParseContext.Document d, Analyzer analyzer, MemoryIndex memoryIndex) {
        for (IndexableField field : d.getFields()) {
            if (!field.fieldType().indexed() && field.name().equals(UidFieldMapper.NAME)) {
                continue;
            }
            try {
                TokenStream tokenStream = field.tokenStream(analyzer);
                if (tokenStream != null) {
                    memoryIndex.addField(field.name(), tokenStream, field.boost());
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to create token stream", e);
            }
        }
        return memoryIndex;
    }

    class DocSearcher implements Engine.Searcher {

        private final IndexSearcher searcher;

        private DocSearcher(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        @Override
        public String source() {
            return "percolate";
        }

        @Override
        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public boolean release() throws ElasticsearchException {
            try {
                searcher.getIndexReader().close();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close IndexReader in percolator with nested doc", e);
            }
            return true;
        }

    }
}
