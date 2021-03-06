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
package com.facebook.presto.cost;

import com.facebook.presto.sql.planner.Symbol;

import java.util.function.Consumer;

import static com.facebook.presto.cost.EstimateAssertion.assertEstimateEquals;
import static com.google.common.collect.Sets.union;
import static org.testng.Assert.assertTrue;

public class PlanNodeStatsAssertion
{
    private final PlanNodeStatsEstimate actual;

    private PlanNodeStatsAssertion(PlanNodeStatsEstimate actual)
    {
        this.actual = actual;
    }

    public static PlanNodeStatsAssertion assertThat(PlanNodeStatsEstimate actual)
    {
        return new PlanNodeStatsAssertion(actual);
    }

    public PlanNodeStatsAssertion outputRowsCount(double expected)
    {
        assertEstimateEquals(actual.getOutputRowCount(), expected, "outputRowsCount mismatch");
        return this;
    }

    public PlanNodeStatsAssertion outputRowsCountUnknown()
    {
        assertTrue(Double.isNaN(actual.getOutputRowCount()), "expected unknown outputRowsCount but got " + actual.getOutputRowCount());
        return this;
    }

    public PlanNodeStatsAssertion symbolStats(String symbolName, Consumer<SymbolStatsAssertion> symbolStatsAssertionConsumer)
    {
        return symbolStats(new Symbol(symbolName), symbolStatsAssertionConsumer);
    }

    public PlanNodeStatsAssertion symbolStats(Symbol symbol, Consumer<SymbolStatsAssertion> columnAssertionConsumer)
    {
        SymbolStatsAssertion columnAssertion = SymbolStatsAssertion.assertThat(actual.getSymbolStatistics(symbol));
        columnAssertionConsumer.accept(columnAssertion);
        return this;
    }

    public PlanNodeStatsAssertion symbolStatsUnknown(String symbolName)
    {
        return symbolStatsUnknown(new Symbol(symbolName));
    }

    public PlanNodeStatsAssertion symbolStatsUnknown(Symbol symbol)
    {
        return symbolStats(symbol,
                columnStats -> columnStats
                        .lowValueUnknown()
                        .highValueUnknown()
                        .nullsFractionUnknown()
                        .distinctValuesCountUnknown());
    }

    public PlanNodeStatsAssertion equalTo(PlanNodeStatsEstimate expected)
    {
        assertEstimateEquals(actual.getOutputRowCount(), expected.getOutputRowCount(), "outputRowCount mismatch");

        for (Symbol symbol : union(expected.getSymbolsWithKnownStatistics(), actual.getSymbolsWithKnownStatistics())) {
            assertSymbolStatsEqual(symbol, actual.getSymbolStatistics(symbol), expected.getSymbolStatistics(symbol));
        }
        return this;
    }

    private void assertSymbolStatsEqual(Symbol symbol, SymbolStatsEstimate actual, SymbolStatsEstimate expected)
    {
        assertEstimateEquals(actual.getNullsFraction(), expected.getNullsFraction(), "nullsFraction mismatch for " + symbol.getName());
        assertEstimateEquals(actual.getLowValue(), expected.getLowValue(), "lowValue mismatch for " + symbol.getName());
        assertEstimateEquals(actual.getHighValue(), expected.getHighValue(), "highValue mismatch for " + symbol.getName());
        assertEstimateEquals(actual.getDistinctValuesCount(), expected.getDistinctValuesCount(), "distinct values count mismatch for " + symbol.getName());
        assertEstimateEquals(actual.getAverageRowSize(), expected.getAverageRowSize(), "average row size mismatch for " + symbol.getName());
    }
}
