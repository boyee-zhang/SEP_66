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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ReadRowsHelper
{
    private BigQueryStorageClient client;
    private ReadRowsRequest.Builder request;
    private int maxReadRowsRetries;

    public ReadRowsHelper(BigQueryStorageClient client, ReadRowsRequest.Builder request, int maxReadRowsRetries)
    {
        this.client = requireNonNull(client, "client cannot be null");
        this.request = requireNonNull(request, "client cannot be null");
        this.maxReadRowsRetries = maxReadRowsRetries;
    }

    public Iterator<ReadRowsResponse> readRows()
    {
        StreamPosition.Builder readPosition = request.getReadPositionBuilder();
        List<ReadRowsResponse> readRowResponses = new ArrayList<>();
        long readRowsCount = 0;
        int retries = 0;
        Iterator<ReadRowsResponse> serverResponses = fetchResponses(request);
        while (serverResponses.hasNext()) {
            try {
                ReadRowsResponse response = serverResponses.next();
                readRowsCount += response.getRowCount();
                readRowResponses.add(response);
            }
            catch (RuntimeException e) {
                // if relevant, retry the read, from the last read position
                if (BigQueryUtil.isRetryable(e) && retries < maxReadRowsRetries) {
                    serverResponses = fetchResponses(request.setReadPosition(readPosition.setOffset(readRowsCount)));
                    retries++;
                }
                else {
                    // to safely close the client
                    try (BigQueryStorageClient ignored = client) {
                        throw e;
                    }
                }
            }
        }
        return readRowResponses.iterator();
    }

    // In order to enable testing
    protected Iterator<ReadRowsResponse> fetchResponses(ReadRowsRequest.Builder readRowsRequest)
    {
        return client.readRowsCallable()
                .call(readRowsRequest.build())
                .iterator();
    }
}
