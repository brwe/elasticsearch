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
package org.elasticsearch.search.fetch.analyzed_text;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.allterms.AllTermsShardRequest;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.termvectors.ShardTermVectorService;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.MatrixScanResult;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.allterms.TransportAllTermsShardAction.getTerms;


/**
 * Query sub phase which pulls data from term vectors (using the cache if
 * available, building it if not).
 * <p/>
 * term_vectors must be enabled:
 * <p/>
 * <pre>
 *  "mappings": {
 *      "review": {
 *          "properties": {
 *              "text": {
 *                  "type": "string",
 *                  "term_vector": "with_positions_offsets_payloads"
 *              }
 *          }
 *      }
 *  }
 * </pre>
 * <p/>
 * and the query parameter is:
 * <pre>
 *  GET movie-reviews/_search
 *  {
 *      "analyzed_text": [
 *      {
 *          "field": "text",  <- the field for which we want the analyzed text
 *          "idf_threshold": 0.5, <- skip terms with idf below idf_threshold (the, a, who, ....)
 *          "df_threshold": 5 <- skip terms with df below df_threshold because they are so rare that they might be typos or uninteresting
 *      },
 *      ...
 *      ]
 *  }
 * </pre>
 */
public class AnalyzedTextFetchSubPhase implements FetchSubPhase {
    ESLogger logger = ESLoggerFactory.getLogger("[analyzed_text] ", "org.elasticsearch.search.fetch.analyzed_text");

    @Inject
    public AnalyzedTextFetchSubPhase() {
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("analyzed_text", new AnalyzedTextParseElement())
                .put("analyzedText", new AnalyzedTextParseElement());
        return parseElements.build();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.hasAnalyzedTextFields();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticsearchException {

        for (AnalyzedTextContext.AnalyzedTextField field : context.analyzedTextFields().fields()) {
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<String, SearchHitField>(2));
            }
            SearchHitField hitField = hitContext.hit().fields().get(field.name());
            if (hitField == null) {
                hitField = new InternalSearchHitField(field.name(), new ArrayList<>(2));
                hitContext.hit().fields().put(field.name(), hitField);
            }
            String[] text = new String[0];

            ShardTermVectorService termVectorsService = context.termVectorService(context.shardTarget().shardId());
            TermVectorResponse termVectorsResponse = termVectorsService.getTermVector(new TermVectorRequest(context.shardTarget().index(), hitContext.hit().type(), hitContext.hit().getId()).termStatistics(true), context.shardTarget().index());
            try {
                if (termVectorsResponse.isExists() && termVectorsResponse.getFields().size() != 0) {

                    String tokenCountField = field.getTokenCountField();
                    if (tokenCountField != null) {
                        FieldMapper mapper = context.mapperService().smartNameFieldMapper(tokenCountField);
                        if (mapper != null) {
                            AtomicFieldData data = context.fieldData().getForField(mapper).load(hitContext.readerContext());
                            ScriptDocValues values = data.getScriptValues();
                            values.setNextDocId(hitContext.docId());
                            text = new String[((Number) values.getValues().get(0)).intValue()];
                        }
                    }
                    final CharsRefBuilder spare = new CharsRefBuilder();
                    Terms terms = termVectorsResponse.getFields().terms(field.name());
                    TermsEnum termIter = terms.iterator(null);
                    BytesRef term = termIter.next();
                    DocsAndPositionsEnum posEnum;
                    while (term != null) {
                        float idf = (float) terms.getDocCount() / termIter.docFreq();
                        spare.copyUTF8Bytes(term);
                        if (logger.isTraceEnabled()) {
                            logger.trace("term: {}, idfthreshold: {}, idf: {}", spare.toString(), field.getIdfThreshold(), Math.log(idf));
                            logger.trace("term: {}, dfthreshold: {}, df: {}", spare.toString(), field.getDfThreshold(), termIter.docFreq());
                        }
                        if ((idf > field.getIdfThreshold()) && (termIter.docFreq() > field.getDfThreshold())) {
                            posEnum = termIter.docsAndPositions(null, null);
                            for (int i = 0; i < posEnum.freq(); i++) {
                                int pos = posEnum.nextPosition();
                                text = add(text, spare.toString(), pos);
                            }
                        }
                        term = termIter.next();
                    }
                    List<String> cleanText = new ArrayList<>(text.length);
                    for (String s : text) {
                        if (s != null) {
                            cleanText.add(s);
                        }
                    }
                    hitField.values().addAll(cleanText);
                }
            } catch (IOException e) {
            }
        }

    }

    public void matrixScanExecute(SearchContext context, MatrixScanResult matrixScanResult) throws ElasticsearchException {
        logger.info("dictionary is: {}", context.getDictionary());
        List<String> terms = new ArrayList<>();
        getTerms("field", context.getMatrixFrom(), 1000, 0, context.indexShard().shardId(), terms, context.searcher());
        for (String term : terms) {
            matrixScanResult.addRow(term, new long[0]);
        }

    }

    private String[] add(String[] text, String s, int pos) {
        if (pos >= text.length) {
            String[] newList = new String[Math.max(pos + 1, text.length * 2)];
            for (int i = 0; i < text.length; i++) {
                newList[i] = text[i];
            }
            text = newList;
        }
        text[pos] = s;
        return text;
    }
}
