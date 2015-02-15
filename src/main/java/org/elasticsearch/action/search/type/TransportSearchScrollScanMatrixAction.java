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

package org.elasticsearch.action.search.type;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.fetch.MatrixScanResult;
import org.elasticsearch.search.fetch.MatrixScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.internalMatrixScrollSearchRequest;
import static org.elasticsearch.action.search.type.TransportSearchHelper.internalScrollSearchRequest;

/**
 *
 */
public class TransportSearchScrollScanMatrixAction extends AbstractComponent {

    protected final ClusterService clusterService;
    protected final SearchServiceTransportAction searchService;
    protected final SearchPhaseController searchPhaseController;

    @Inject
    public TransportSearchScrollScanMatrixAction(Settings settings, ClusterService clusterService,
                                                 SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings);
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
    }

    public void execute(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        new AsyncAction(request, scrollId, listener).start();
    }

    class AsyncAction extends AbstractAsyncAction {

        final SearchScrollRequest request;

        final ActionListener<SearchResponse> listener;

        final ParsedScrollId scrollId;

        final DiscoveryNodes nodes;

        private volatile AtomicArray<ShardSearchFailure> shardFailures;
        final AtomicArray<MatrixScanResult> matrixScanResult;

        final AtomicInteger successfulOps;
        final AtomicInteger counter;

        AsyncAction(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.scrollId = scrollId;
            this.nodes = clusterService.state().nodes();
            this.successfulOps = new AtomicInteger(scrollId.getContext().length);
            this.counter = new AtomicInteger(scrollId.getContext().length);

            this.matrixScanResult = new AtomicArray<>(scrollId.getContext().length);
        }

        protected final ShardSearchFailure[] buildShardFailures() {
            if (shardFailures == null) {
                return ShardSearchFailure.EMPTY_ARRAY;
            }
            List<AtomicArray.Entry<ShardSearchFailure>> entries = shardFailures.asList();
            ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
            for (int i = 0; i < failures.length; i++) {
                failures[i] = entries.get(i).value;
            }
            return failures;
        }

        // we do our best to return the shard failures, but its ok if its not fully concurrently safe
        // we simply try and return as much as possible
        protected final void addShardFailure(final int shardIndex, ShardSearchFailure failure) {
            if (shardFailures == null) {
                shardFailures = new AtomicArray<>(scrollId.getContext().length);
            }
            shardFailures.set(shardIndex, failure);
        }

        public void start() {
            if (scrollId.getContext().length == 0) {
                final InternalSearchResponse internalResponse = new InternalSearchResponse(new InternalSearchHits(InternalSearchHits.EMPTY, Long.parseLong(this.scrollId.getAttributes().get("total_hits")), 0.0f), null, null, null, null, false, null);
                listener.onResponse(new SearchResponse(internalResponse, request.scrollId(), 0, 0, 0l, buildShardFailures()));
                return;
            }

            Tuple<String, Long>[] context = scrollId.getContext();
            for (int i = 0; i < context.length; i++) {
                Tuple<String, Long> target = context[i];
                DiscoveryNode node = nodes.get(target.v1());
                if (node != null) {
                    executePhase(i, node, target.v2());
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.getSource() + "]");
                    }
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }
            }

            for (Tuple<String, Long> target : scrollId.getContext()) {
                DiscoveryNode node = nodes.get(target.v1());
                if (node == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.getSource() + "]");
                    }
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                } else {
                }
            }
        }

        void executePhase(final int shardIndex, DiscoveryNode node, final long searchId) {
            searchService.sendExecuteMatrixScan(node, internalMatrixScrollSearchRequest(searchId, request), new SearchServiceListener<MatrixScanResult>() {
                @Override
                public void onResult(MatrixScanResult result) {
                    matrixScanResult.set(shardIndex, result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    onPhaseFailure(t, searchId, shardIndex);
                }
            });
        }

        void onPhaseFailure(Throwable t, long searchId, int shardIndex) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Failed to execute query phase", t, searchId);
            }
            addShardFailure(shardIndex, new ShardSearchFailure(t));
            successfulOps.decrementAndGet();
            if (counter.decrementAndGet() == 0) {
                finishHim();
            }
        }

        private void finishHim() {
            try {
                innerFinishHim();
            } catch (Throwable e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("fetch", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                listener.onFailure(failure);
            }
        }

        private void innerFinishHim() throws IOException {
            MatrixScanResult finalResult = new MatrixScanResult(0, null);
            if (matrixScanResult.length() != 0) {

                boolean done = false;
                List<List<Tuple<String, long[]>>> iterators = getIterators(matrixScanResult);
                int[] index = new int[iterators.size()];
                while (true) {
                    String term = getMinimum(iterators, index);
                    if (term == null) {
                        break;
                    }
                    finalResult.addRow(term, new long[0]);
                    move(iterators, index, term);
                }
            }
            listener.onResponse(new SearchResponse(new InternalSearchResponse(InternalSearchHits.empty(), null, null, null, finalResult, false, false), scrollId.getSource(), this.scrollId.getContext().length, successfulOps.get(),
                    buildTookInMillis(), buildShardFailures()));
        }

        private void move(List<List<Tuple<String, long[]>>> iterators, int[] index, String term) {
            for (int i = 0; i < iterators.size(); i++) {
                if (iterators.get(i).size() > index[i]) {
                    String other = iterators.get(i).get(index[i]).v1();
                    if (term.compareTo(other) == 0) {
                        index[i] = index[i] + 1;
                    }
                }
            }
        }

        private String getMinimum(List<List<Tuple<String, long[]>>> iterators, int[] index) {

            String minTerm = null;
            for (int i = 0; i < iterators.size(); i++) {
                if (index[i] < iterators.get(i).size()) {
                    String other = iterators.get(i).get(index[i]).v1();
                    if (minTerm == null) {
                        minTerm = other;
                    } else {
                        if (minTerm.compareTo(other) > 0) {
                            minTerm = other;
                        }
                    }
                }
            }
            return minTerm;
        }

        private List<List<Tuple<String, long[]>>> getIterators(AtomicArray<MatrixScanResult> matrixScanResults) {

            ArrayList<List<Tuple<String, long[]>>> iterators = new ArrayList<>();
            iterators.ensureCapacity(matrixScanResults.length());
            for (AtomicArray.Entry<MatrixScanResult> matrixScanResult : matrixScanResults.asList()) {
                iterators.add(matrixScanResult.value.shardTarget().getShardId(), matrixScanResult.value.getPostingLists());
            }
            return iterators;
        }
    }
}
