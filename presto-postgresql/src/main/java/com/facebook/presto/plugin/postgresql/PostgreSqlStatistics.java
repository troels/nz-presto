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

import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class PostgreSqlStatistics
{
    private final long numberOfRows;
    private final String primaryKeyColumn;
    private final List<Object> histogram;
    private final Type primaryKeyType;

    public PostgreSqlStatistics(long numberOfRows, String primaryKeyColumn, Type primaryKeyType, List<Object> histogram)
    {
        this.numberOfRows = numberOfRows;
        this.primaryKeyColumn = primaryKeyColumn;
        this.histogram = histogram;
        this.primaryKeyType = primaryKeyType;
    }

    public long getNumberOfRows()
    {
        return numberOfRows;
    }

    public String getPrimaryKeyColumn()
    {
        return primaryKeyColumn;
    }

    public Type getPrimaryKeyType()
    {
        return primaryKeyType;
    }

    public List<Object> getHistogram()
    {
        return histogram;
    }

    public List<Range> getHistogramRanges()
    {
        if (histogram.size() == 0) {
            return ImmutableList.of(Range.all(primaryKeyType));
        }
        ImmutableList.Builder<Range> ranges = ImmutableList.builder();

        ranges.add(Range.lessThan(primaryKeyType, histogram.get(0)));
        for (int i = 1; i < histogram.size(); i++) {
            ranges.add(Range.range(primaryKeyType, histogram.get(i - 1), true, histogram.get(i), false));
        }
        ranges.add(Range.greaterThanOrEqual(primaryKeyType, histogram.get(histogram.size() - 1)));
        return ranges.build();
    }
}
