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

package org.elasticsearch.action.allterms;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportAllTermsShardAction extends TransportShardSingleOperationAction<AllTermsShardRequest, AllTermsSingleShardResponse> {

    private final IndicesService indicesService;

    private static final String ACTION_NAME = AllTermsAction.NAME + "[s]";


    @Inject
    public TransportAllTermsShardAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                        IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters);
        this.indicesService = indicesService;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GET;
    }

    @Override
    protected AllTermsShardRequest newRequest() {
        return new AllTermsShardRequest();
    }

    @Override
    protected AllTermsSingleShardResponse newResponse() {
        return new AllTermsSingleShardResponse(null);
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
                .getShards(state, request.concreteIndex(), request.request().shardId(), request.request().preference());
    }

    @Override
    protected AllTermsSingleShardResponse shardOperation(AllTermsShardRequest request, ShardId shardId) throws ElasticsearchException {
        List<String> terms = new ArrayList<>();
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        final Engine.Searcher searcher = indexShard.acquireSearcher("all_terms");
        getTerms(request.field(), request.from(), request.size, request.minDocFreq(), shardId, terms, searcher);
        return new AllTermsSingleShardResponse(terms);
    }

    public static void getTerms(String field, String from, int size, long minDocFreq, ShardId shardId, List<String> terms, IndexSearcher searcher) {
        IndexReader topLevelReader = searcher.getIndexReader();
        getTerms(field, from, size, minDocFreq, shardId, terms, topLevelReader, false);
    }

    public static void getTerms(String field, String from, int size, long minDocFreq, ShardId shardId, List<String> terms, Engine.Searcher searcher) {
        IndexReader topLevelReader = searcher.reader();
        try {
            if (getTerms(field, from, size, minDocFreq, shardId, terms, topLevelReader, true)) return;
        } finally {
            searcher.close();
        }
    }

    protected static boolean getTerms(String field, String from, int size, long minDocFreq, ShardId shardId, List<String> terms, IndexReader topLevelReader, boolean includeFrom) {
        List<AtomicReaderContext> leaves = topLevelReader.leaves();
        if (leaves.size() == 0) {
            return true;
        }
        List<TermsEnum> termIters = new ArrayList<>();

        try {
            for (AtomicReaderContext reader : leaves) {
                termIters.add(reader.reader().terms(field).iterator(null));
            }
        } catch (IOException e) {
        }
        CharsRefBuilder spare = new CharsRefBuilder();
        BytesRef lastTerm = null;
        int[] exhausted = new int[termIters.size()];
        for (int i = 0; i < exhausted.length; i++) {
            exhausted[i] = 0;
        }
        try {
            //first find smallest term
            lastTerm = findInitialMinimum(from, termIters, lastTerm, exhausted, includeFrom);
            if (lastTerm == null) {
                return true;
            }
            if (getDocFreq(termIters, lastTerm, field, exhausted) >= minDocFreq) {
                spare.copyUTF8Bytes(lastTerm);
                terms.add(spare.toString());
            }
            BytesRef blah = new BytesRef();
            blah.copyBytes(lastTerm);
            lastTerm = blah;

            while (terms.size() < size && lastTerm != null) {
                moveIterators(exhausted, termIters, lastTerm, shardId);
                lastTerm = findMinimum(exhausted, termIters, shardId);

                if (lastTerm != null) {

                    if (getDocFreq(termIters, lastTerm, field, exhausted) >= minDocFreq) {
                        spare.copyUTF8Bytes(lastTerm);
                        terms.add(spare.toString());
                    }
                }
            }
        } catch (IOException e) {
        }
        return false;
    }

    private static BytesRef findInitialMinimum(String from, List<TermsEnum> termIters, BytesRef lastTerm, int[] exhausted, boolean includeFrom) throws IOException {
        for (int i = 0; i < termIters.size(); i++) {
            BytesRef curTerm = null;
            if (from != null) {
                TermsEnum.SeekStatus seekStatus = termIters.get(i).seekCeil(new BytesRef(from));
                if (seekStatus.equals(TermsEnum.SeekStatus.FOUND) == true && (includeFrom == false)) {
                    curTerm = termIters.get(i).next();
                    if (curTerm == null) {
                        exhausted[i] = 1;
                    }
                } else if (seekStatus.equals(TermsEnum.SeekStatus.END) == false) {
                    curTerm = termIters.get(i).term();
                } else {
                    exhausted[i] = 1;
                }
            } else {
                curTerm = termIters.get(i).next();
            }

            if (lastTerm == null) {
                lastTerm = curTerm;
                if (lastTerm == null || lastTerm.length == 0) {
                    lastTerm = null;
                    exhausted[i] = 1;
                }
            } else {
                if (curTerm != null) {
                    if (curTerm.compareTo(lastTerm) < 0) {
                        lastTerm = curTerm;
                    }
                }
            }
        }
        return lastTerm;
    }

    private static long getDocFreq(List<TermsEnum> termIters, BytesRef lastTerm, String field, int[] exhausted) {
        long docFreq = 0;

        for (int i = 0; i < termIters.size(); i++) {
            if (exhausted[i] == 0) {
                try {
                    if (termIters.get(i).term().compareTo(lastTerm) == 0) {
                        docFreq += termIters.get(i).docFreq();
                    }
                } catch (IOException e) {

                }
            }
        }
        return docFreq;
    }

    private static BytesRef findMinimum(int[] exhausted, List<TermsEnum> termIters, ShardId shardId) {
        BytesRef minTerm = null;
        for (int i = 0; i < termIters.size(); i++) {
            if (exhausted[i] == 1) {
                continue;
            }
            BytesRef candidate = null;
            try {
                candidate = termIters.get(i).term();
            } catch (IOException e) {
            }
            if (minTerm == null) {
                minTerm = candidate;

            } else {
                //it is actually smaller, so we add it
                if (minTerm.compareTo(candidate) > 0) {
                    minTerm = candidate;
                }
            }

        }
        if (minTerm != null) {
            BytesRef ret = new BytesRef();
            ret.copyBytes(minTerm);
            return ret;
        }
        return null;
    }

    private static void moveIterators(int[] exhausted, List<TermsEnum> termIters, BytesRef lastTerm, ShardId shardId) {

        try {
            for (int i = 0; i < termIters.size(); i++) {
                if (exhausted[i] == 1) {
                    continue;
                }
                CharsRefBuilder toiString = new CharsRefBuilder();
                toiString.copyUTF8Bytes(lastTerm);
                BytesRef candidate;
                if (termIters.get(i).term().compareTo(lastTerm) == 0) {
                    candidate = termIters.get(i).next();
                } else {
                    //it must stand on one that is greater so we just get it
                    candidate = termIters.get(i).term();
                }
                if (candidate == null) {
                    exhausted[i] = 1;
                } else {
                    toiString = new CharsRefBuilder();
                    toiString.copyUTF8Bytes(candidate.clone());
                }
            }
        } catch (IOException e) {

        }
    }
}
