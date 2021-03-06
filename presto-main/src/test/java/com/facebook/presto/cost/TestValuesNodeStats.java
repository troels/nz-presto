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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.lang.Double.NaN;

public class TestValuesNodeStats
{
    private StatsCalculatorTester tester;

    @BeforeMethod
    public void setUp()
    {
        tester = new StatsCalculatorTester();
    }

    @AfterMethod
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testStatsForValuesNode()
            throws Exception
    {
        tester.assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT), pb.symbol("b", DOUBLE)),
                        ImmutableList.of(
                                ImmutableList.of(expression("3+3"), expression("13.5")),
                                ImmutableList.of(expression("55"), expression("null")),
                                ImmutableList.of(expression("6"), expression("13.5")))))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(3)
                                .addSymbolStatistics(
                                        new Symbol("a"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0)
                                                .setLowValue(6)
                                                .setHighValue(55)
                                                .setDistinctValuesCount(2)
                                                .build())
                                .addSymbolStatistics(
                                        new Symbol("b"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0.33333333333333333)
                                                .setLowValue(13.5)
                                                .setHighValue(13.5)
                                                .setDistinctValuesCount(1)
                                                .build())
                                .build()));
    }

    @Test
    public void testStatsForValuesNodeWithJustNulls()
            throws Exception
    {
        PlanNodeStatsEstimate nullAStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1)
                .addSymbolStatistics(
                        new Symbol("a"),
                        SymbolStatsEstimate.builder()
                                .setLowValue(NaN)
                                .setHighValue(NaN)
                                .setNullsFraction(1.0)
                                .setDistinctValuesCount(0.0)
                                .build())
                .build();

        tester.assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                        ImmutableList.of(
                                ImmutableList.of(expression("3 + null")))))
                .check(outputStats -> outputStats.equalTo(nullAStats));

        tester.assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                        ImmutableList.of(
                                ImmutableList.of(expression("null")))))
                .check(outputStats -> outputStats.equalTo(nullAStats));

        tester.assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", UNKNOWN)),
                        ImmutableList.of(
                                ImmutableList.of(expression("null")))))
                .check(outputStats -> outputStats.equalTo(nullAStats));
    }

    @Test
    public void testStatsForEmptyValues()
            throws Exception
    {
        tester.assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                        ImmutableList.of()))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(0)
                                .addSymbolStatistics(
                                        new Symbol("a"),
                                        SymbolStatsEstimate.builder()
                                                .setLowValue(NaN)
                                                .setHighValue(NaN)
                                                .setNullsFraction(0.0)
                                                .setDistinctValuesCount(0.0)
                                                .build())
                                .build()));
    }
}
