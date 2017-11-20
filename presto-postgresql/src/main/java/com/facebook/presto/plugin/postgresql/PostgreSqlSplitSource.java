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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.fromProperties;

public class PostgreSqlSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(PostgreSqlSplitSource.class);

    private static final int MAX_ROWS_GOAL_PER_SPLIT = 100_000;

    private final String connectorId;
    private final JdbcTableHandle tableHandle;
    private final String connectionUrl;
    private final Properties properties;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final PostgreSqlClient client;
    private Optional<PostgreSqlStatistics> statistics = Optional.empty();
    private boolean closed = false;

    public PostgreSqlSplitSource(String connectorId,
                                 JdbcTableHandle tableHandle, String connectionUrl,
                                 Properties properties, TupleDomain<ColumnHandle> tupleDomain,
                                 PostgreSqlClient client)
    {
        this.connectorId = connectorId;
        this.tableHandle = tableHandle;
        this.connectionUrl = connectionUrl;
        this.properties = properties;
        this.tupleDomain = tupleDomain;
        this.client = client;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        checkState(!closed, "Connector is closed");
        getStatistics();
        final Map<String, String> mapProperties = fromProperties(properties);

        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(ImmutableMap.of());

        ColumnHandle primaryKey = null;
        PostgreSqlStatistics rawStatistics = null;
        List<Range> histogramRanges = null;
        long numberOfRows = 0;
        long rowsPerRange = 0;

        if (statistics.isPresent()) {
            rawStatistics = statistics.get();

            primaryKey = new JdbcColumnHandle(
                    connectorId,
                    rawStatistics.getPrimaryKeyColumn(),
                    rawStatistics.getPrimaryKeyType());
            histogramRanges = rawStatistics.getHistogramRanges();
            numberOfRows = rawStatistics.getNumberOfRows();
            if (histogramRanges.size() != 0) {
                rowsPerRange = numberOfRows / histogramRanges.size();
            }
        }

        if (!statistics.isPresent() || domains.containsKey(primaryKey) || numberOfRows == 0 || rowsPerRange == 0) {
            closed = true;
            return CompletableFuture.completedFuture(
                    ImmutableList.of(
                            new JdbcSplit(
                                    connectorId,
                                    tableHandle.getCatalogName(),
                                    tableHandle.getSchemaName(),
                                    tableHandle.getTableName(),
                                    connectionUrl,
                                    mapProperties,
                                    tupleDomain)));
        }

        long rangesPerSplit = MAX_ROWS_GOAL_PER_SPLIT / rowsPerRange;

        if (rangesPerSplit == 0) {
            rangesPerSplit = 1;
        }

        ImmutableList.Builder<Range> ranges = ImmutableList.builder();
        Range currentRange = null;
        int currentRanges = 0;
        for (Range histogramRange : histogramRanges) {
            if (currentRange == null) {
                currentRange = histogramRange;
            }
            else {
                currentRange = currentRange.span(histogramRange);
            }

            currentRanges++;
            if (currentRanges == rangesPerSplit) {
                currentRanges = 0;
                ranges.add(currentRange);
                currentRange = null;
            }
        }

        if (currentRange != null) {
            ranges.add(currentRange);
        }

        Type primaryKeyType = rawStatistics.getPrimaryKeyType();
        final ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (Range range : ranges.build()) {
            TupleDomain<ColumnHandle> intersect = tupleDomain.intersect(
                    TupleDomain.withColumnDomains(
                            ImmutableMap.of(
                                    primaryKey,
                                    Domain.create(
                                            ValueSet.copyOfRanges(primaryKeyType, ImmutableList.of(range)),
                                            false))));
            splits.add(
                    new JdbcSplit(
                            connectorId,
                            tableHandle.getCatalogName(),
                            tableHandle.getSchemaName(),
                            tableHandle.getTableName(),
                            connectionUrl,
                            mapProperties,
                            intersect));
        }
        closed = true;
        return CompletableFuture.completedFuture(splits.build());
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    private void getStatistics()
    {
        if (statistics.isPresent()) {
            return;
        }

        statistics = client.getStatistics(tableHandle, connectionUrl, properties);
    }
}
