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

package org.elasticsearch.index.store;

import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.concurrent.atomic.AtomicBoolean;


public class MockTransportServiceWithExceptionOnResponse extends MockTransportService {

    final static AtomicBoolean exceptionThrown = new AtomicBoolean(false);

    @Inject
    public MockTransportServiceWithExceptionOnResponse(Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings, transport, threadPool);
    }


    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request,
                                                          final TransportRequestOptions options, TransportResponseHandler<T> handler) {
        super.sendRequest(node, action, request, options, new MockHandler(handler));
    }

    public static class MockHandler implements TransportResponseHandler {
        TransportResponseHandler handler;

        public MockHandler(TransportResponseHandler handler) {
            this.handler = handler;

        }

        @Override
        public TransportResponse newInstance() {
            return handler.newInstance();
        }

        @Override
        public void handleResponse(TransportResponse response) {
            if ((response instanceof BulkShardResponse) && (exceptionThrown.get() == false)) {
                handleException(new CustomTransportException("This is a test exception", new IndexMissingException(new Index("test"))));
                exceptionThrown.set(true);
            } else {
                handler.handleResponse(response);
            }

        }

        @Override
        public void handleException(TransportException exp) {
            handler.handleException(exp);
        }

        @Override
        public String executor() {
            return handler.executor();
        }


    }

    public static class CustomTransportException extends TransportException implements ElasticsearchWrapperException {

        public CustomTransportException(String msg) {
            super(msg);
        }

        public CustomTransportException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
