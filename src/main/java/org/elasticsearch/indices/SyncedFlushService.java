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
package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.synccommit.SyncCommitRequest;
import org.elasticsearch.action.admin.indices.synccommit.SyncCommitResponse;
import org.elasticsearch.action.admin.indices.synccommit.TransportSyncCommitAction;
import org.elasticsearch.action.synccommit.TransportWriteSyncCommitAction;
import org.elasticsearch.action.synccommit.WriteSyncCommitRequest;
import org.elasticsearch.action.synccommit.WriteSyncCommitResponse;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ExecutionException;

public class SyncedFlushService extends AbstractComponent {

    private final TransportSyncCommitAction transportSyncCommitAction;
    private final TransportWriteSyncCommitAction transportWriteSyncCommitAction;

    @Inject
    public SyncedFlushService(Settings settings, TransportSyncCommitAction transportSyncCommitAction, TransportWriteSyncCommitAction transportWriteSyncCommitAction) {
        super(settings);
        this.transportSyncCommitAction = transportSyncCommitAction;
        this.transportWriteSyncCommitAction = transportWriteSyncCommitAction;
    }

    public WriteSyncCommitResponse attemptSyncedFlush(ShardId shardId) throws ExecutionException, InterruptedException {
        SyncCommitResponse syncCommitResponse = transportSyncCommitAction.execute(new SyncCommitRequest(shardId)).get();
        String syncId = "123";
        WriteSyncCommitResponse writeSyncCommitResponse = transportWriteSyncCommitAction.execute(new WriteSyncCommitRequest(shardId, syncId, syncCommitResponse.commitIds())).get();
        return writeSyncCommitResponse;
    }
}
