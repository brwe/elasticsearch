/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.index.memindex;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;

/**
 */
public class MemoryIndexService extends AbstractComponent {

    private final CloseableThreadLocal<MemoryIndex> cache;
    private final IndicesService indicesService;

    @Inject
    public MemoryIndexService(Settings settings, IndicesService indicesService) {
        super(settings);
        this.indicesService = indicesService;
        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                return new ExtendedMemoryIndex(true, maxReuseBytes);
            }
        };
    }

    public class MemoryIndexContext {

        final MemoryIndex memoryIndex;

        public MemoryIndexContext(MemoryIndex memoryIndex) {
            this.memoryIndex = memoryIndex;
        }

        public IndexSearcher indexSearcher() {
            return memoryIndex.createSearcher();
        }

        public void close() {
            memoryIndex.reset();
        }


    }

    public MemoryIndexContext getIndexForDocument(SourceToParse doc, DocumentMapper documentMapper) {
        ParsedDocument parsedDocument = documentMapper.parse(doc);
        final MemoryIndex memoryIndex = cache.get();
        for (IndexableField field : parsedDocument.rootDoc().getFields()) {
            if (!field.fieldType().indexed()) {
                continue;
            }
            // no need to index the UID field
            if (field.name().equals(UidFieldMapper.NAME)) {
                continue;
            }
            TokenStream tokenStream;
            try {
                tokenStream = field.tokenStream(parsedDocument.analyzer());
                if (tokenStream != null) {

                    memoryIndex.addField(field.name(), tokenStream, field.boost());
                }
            } catch (IOException e) {
                throw new ElasticSearchException("Failed to create token stream", e);
            }
        }

        return new MemoryIndexContext(memoryIndex);
    }

    public void close() {
        cache.close();
    }


}
