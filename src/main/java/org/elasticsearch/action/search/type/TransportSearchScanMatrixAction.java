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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.allterms.AllTermsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.action.search.type.TransportSearchHelper.buildScrollId;
import static org.elasticsearch.action.search.type.TransportSearchHelper.internalMatrixScanRequest;

public class TransportSearchScanMatrixAction extends TransportSearchTypeAction {

    @Inject
    public TransportSearchScanMatrixAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                           SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController, ActionFilters actionFilters) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController, actionFilters);
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<QuerySearchResult> {

        String[] dictionary = null;

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override
        protected String firstPhaseName() {
            return "init_scan_matrix";
        }

        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, SearchServiceListener<QuerySearchResult> listener) {
            searchService.sendExecuteMatrixScan(node, request, listener);
        }

        @Override
        public void start() {
            if (expectedSuccessfulOps == 0) {
                // no search shards to search on, bail with empty response (it happens with search across _all with no indices around and consistent with broadcast operations)
                listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, buildTookInMillis(), ShardSearchFailure.EMPTY_ARRAY));
                return;
            }
            request.beforeStart();
            int shardIndex = -1;


            // here we probably have to get the dictionary and then
            // send it together with the sendExecuteScan so that it is added to the search contexts on the shards.
            dictionary = getDictionary();

            for (final ShardIterator shardIt : shardsIts) {
                shardIndex++;
                final ShardRouting shard = shardIt.nextOrNull();
                if (shard != null) {
                    performFirstPhase(shardIndex, shardIt, shard);
                } else {
                    // really, no shards active in this group
                    onFirstPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
                }
            }
        }

        @Override
        void performFirstPhase(final int shardIndex, final ShardIterator shardIt, final ShardRouting shard) {
            if (shard == null) {
                // no more active shards... (we should not really get here, but just for safety)
                onFirstPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
            } else {
                final DiscoveryNode node = nodes.get(shard.currentNodeId());
                if (node == null) {
                    onFirstPhaseResult(shardIndex, shard, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
                } else {
                    String[] filteringAliases = clusterState.metaData().filteringAliases(shard.index(), request.indices());
                    // here send the dictionary to the shards...but how?
                    sendExecuteFirstPhase(node, internalMatrixScanRequest(shard, shardsIts.size(), request, filteringAliases, startTime(), useSlowScroll, dictionary), new SearchServiceListener<QuerySearchResult>() {
                        @Override
                        public void onResult(QuerySearchResult result) {
                            onFirstPhaseResult(shardIndex, shard, result, shardIt);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            onFirstPhaseResult(shardIndex, shard, node.id(), shardIt, t);
                        }
                    });
                }
            }
        }


        @Override
        protected void moveToSecondPhase() throws Exception {
            final InternalSearchResponse internalResponse = searchPhaseController.merge(SearchPhaseController.EMPTY_DOCS, firstResults, (AtomicArray<? extends FetchSearchResultProvider>) AtomicArray.empty());
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = buildScrollId(request.searchType(), firstResults, ImmutableMap.of("total_hits", Long.toString(internalResponse.hits().totalHits())));
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
        }

        public String[] getDictionary() {
            String[] dict =  {"blah"};
            return dict;
        }
    }
}
