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
package io.trino.connector;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MockConnectorPageSource
        implements UpdatablePageSource
{
    private final ConnectorPageSource delegate;

    public MockConnectorPageSource(ConnectorPageSource delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return delegate.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    @Override
    public void deleteRows(Block rowIds) {}

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels) {}

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(Collections.emptyList());
    }

    @Override
    public void abort()
    {
        UpdatablePageSource.super.abort();
    }
}
