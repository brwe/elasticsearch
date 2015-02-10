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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.facet.terms.strings.HashedAggregator;
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
        IndexReader topLevelReader = searcher.reader();

        List<AtomicReaderContext> leaves = topLevelReader.leaves();

        try {
            if (leaves.size() == 0) {
                return new AllTermsSingleShardResponse(terms);
            }
            List<TermsEnum> termIters = new ArrayList<>();

            try {
                for (AtomicReaderContext reader : leaves) {
                    termIters.add(reader.reader().terms(request.field()).iterator(null));
                }
            } catch (IOException e) {
            }
            CharsRefBuilder spare = new CharsRefBuilder();
            BytesRef lastTerm = null;
            try {
                //first find smallest term
                for (int i = 0; i < termIters.size(); i++) {
                    BytesRef curTerm = termIters.get(i).next();
                    if (lastTerm == null) {
                        lastTerm = curTerm;
                        if (lastTerm.length ==0) {
                            lastTerm = null;
                        }
                    } else {
                        if (lastTerm.compareTo(curTerm) > 0) {
                            lastTerm = curTerm;
                        }
                    }
                }
                if (lastTerm == null) {
                    return new AllTermsSingleShardResponse(terms);
                }
                spare.copyUTF8Bytes(lastTerm);
                terms.add(spare.toString());
                int exhaustedIters = 0;
                while (terms.size() < request.size()) {
                    BytesRef curTerm = null;
                    for (int i = 0; i < termIters.size(); i++) {
                        if (termIters.get(i) == null) {
                            continue;
                        }
                        BytesRef term;
                        //first, check if we have to move the iterator, might be the iterator currently stands on the last term
                        if (termIters.get(i).term().compareTo(lastTerm) == 0) {
                            spare.copyUTF8Bytes(lastTerm);
                            logger.info("lastTerm {}", spare.toString());
                            term = termIters.get(i).next();


                        } else {
                            //it must stand on one that is greater so we just get it
                            term = termIters.get(i).term();
                        }
                        if (term == null) {
                            termIters.set(i, null);
                            exhaustedIters++;
                        } else {
                            spare.copyUTF8Bytes(term);
                            logger.info("term {}", spare.toString());
                            // we have not assigned anything yet
                            if (curTerm == null) {
                                curTerm = term;
                            } else {
                                //it is actually smaller, so we add it
                                if (curTerm.compareTo(term) < 0) {
                                    curTerm = term;
                                }
                            }
                        }
                    }
                    lastTerm = curTerm;
                    if (exhaustedIters == termIters.size()) {
                        break;
                    }
                    spare.copyUTF8Bytes(lastTerm);
                    terms.add(spare.toString());
                }
            } catch (IOException e) {
            }

            return new AllTermsSingleShardResponse(terms);
        } finally {
            searcher.close();
        }
    }
}
