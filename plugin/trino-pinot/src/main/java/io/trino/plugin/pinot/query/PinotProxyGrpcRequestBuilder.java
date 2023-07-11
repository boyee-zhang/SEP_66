/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.pinot.query;

import com.google.common.collect.ImmutableList;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.spi.utils.CommonConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PinotProxyGrpcRequestBuilder
{
    private static final String KEY_OF_PROXY_GRPC_FORWARD_HOST = "FORWARD_HOST";
    private static final String KEY_OF_PROXY_GRPC_FORWARD_PORT = "FORWARD_PORT";

    private String hostName;
    private int port = -1;
    private String brokerId = "unknown";
    private boolean enableStreaming;
    private String payloadType;
    private String sql;
    private List<String> segments;

    public PinotProxyGrpcRequestBuilder setHostName(String hostName)
    {
        this.hostName = hostName;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setPort(int port)
    {
        this.port = port;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setEnableStreaming(boolean enableStreaming)
    {
        this.enableStreaming = enableStreaming;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setSql(String sql)
    {
        payloadType = CommonConstants.Query.Request.PayloadType.SQL;
        this.sql = sql;
        return this;
    }

    public PinotProxyGrpcRequestBuilder setSegments(List<String> segments)
    {
        this.segments = ImmutableList.copyOf(segments);
        return this;
    }

    public Server.ServerRequest build()
    {
        if (!payloadType.equals(CommonConstants.Query.Request.PayloadType.SQL)) {
            throw new RuntimeException("Only [SQL] Payload type is allowed: " + payloadType);
        }
        Map<String, String> metadata = new HashMap<>();
        metadata.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, "0");
        metadata.put(CommonConstants.Query.Request.MetadataKeys.BROKER_ID, brokerId);
        metadata.put(CommonConstants.Query.Request.MetadataKeys.ENABLE_TRACE, "false");
        metadata.put(CommonConstants.Query.Request.MetadataKeys.ENABLE_STREAMING, Boolean.toString(enableStreaming));
        metadata.put(CommonConstants.Query.Request.MetadataKeys.PAYLOAD_TYPE, payloadType);
        if (this.hostName != null) {
            metadata.put(KEY_OF_PROXY_GRPC_FORWARD_HOST, this.hostName);
        }
        if (this.port > 0) {
            metadata.put(KEY_OF_PROXY_GRPC_FORWARD_PORT, String.valueOf(this.port));
        }
        return Server.ServerRequest.newBuilder()
            .putAllMetadata(metadata)
            .setSql(sql)
            .addAllSegments(segments)
            .build();
    }
}
