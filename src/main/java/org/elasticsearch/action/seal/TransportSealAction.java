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

package org.elasticsearch.action.seal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportSealAction extends TransportMasterNodeOperationAction<SealRequest, SealResponse> {

    private final DestructiveOperations destructiveOperations;
    private final MetaDataIndexStateService indexStateService;

    @Inject
    public TransportSealAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, TransportShardSealAction shardSealAction, ActionFilters actionFilters, NodeSettingsService nodeSettingsService,
                               MetaDataIndexStateService indexStateService) {
        super(settings, SealAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.indexStateService = indexStateService;
        this.destructiveOperations = new DestructiveOperations(logger, settings, nodeSettingsService);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected SealRequest newRequest() {
        return new SealRequest();
    }

    @Override
    protected SealResponse newResponse() {
        return new SealResponse();
    }

    @Override
    protected void doExecute(SealRequest request, ActionListener<SealResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(SealRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    @Override
    protected void masterOperation(final SealRequest request, ClusterState state, final ActionListener<SealResponse> listener) throws Exception {
        final String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        SealIndexClusterStateUpdateRequest updateRequest = new SealIndexClusterStateUpdateRequest()
                .ackTimeout(request.masterNodeTimeout()).masterNodeTimeout(request.masterNodeTimeout())
                .indices(concreteIndices);

        indexStateService.sealIndex(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new SealResponse(response.isAcknowledged(), request.index()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to seal indices [{}]", t, concreteIndices);
                listener.onFailure(t);
            }
        }, IndexMetaData.State.SEALING);
    }
}