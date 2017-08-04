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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.statistics.StatisticsAssertion;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.USE_NEW_STATS_CALCULATOR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.Metrics.OUTPUT_ROW_COUNT;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

/**
 * The tests in this class have been written to ensure that the statistics code
 * does not introduce regressions in selectivity estimates of table rows in TPCH
 * queries in future.
 */
public class TestTpchQueriesSelectivityStats
{
    private final StatisticsAssertion statisticsAssertion;

    public TestTpchQueriesSelectivityStats()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(USE_NEW_STATS_CALCULATOR, "true")
                .build();

        LocalQueryRunner runner = new LocalQueryRunner(defaultSession);
        runner.createCatalog("tpch", new TpchConnectorFactory(1),
                ImmutableMap.of("tpch.column-naming", ColumnNaming.STANDARD.name()
                ));
        statisticsAssertion = new StatisticsAssertion(runner);
    }

    @Test
    void testLineitemQ1()
    {
        statisticsAssertion.check(
                "SELECT * FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when there is support for LIKE in statistics estimation
    @Test(enabled = false)
    void testPartQ2()
    {
        statisticsAssertion.check(
                "SELECT p_partkey, p_mfgr FROM part WHERE p_size = 15 AND p_type like '%BRASS'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when statistics estimation supports this case
    @Test(enabled = false)
    void testCustomerQ3()
    {
        statisticsAssertion.check(
                "SELECT c_custkey FROM customer WHERE c_mktsegment = 'BUILDING'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testLineitemQ3()
    {
        statisticsAssertion.check(
                "SELECT * FROM lineitem WHERE l_shipdate > DATE '1995-03-15'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test()
    void testOrdersQ3()
    {
        statisticsAssertion.check(
                "SELECT * FROM orders WHERE o_orderdate < DATE '1995-03-15'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testOrdersQ4()
    {
        statisticsAssertion.check(
                "SELECT o_orderpriority FROM orders" +
                        " WHERE o_orderdate >= DATE '1993-07-01'" +
                        " AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when statistics estimation supports non-equality comparision between two columns
    @Test(enabled = false)
    void testLineitemQ4()
    {
        statisticsAssertion.check(
                "SELECT * FROM lineitem WHERE l_commitdate < l_receiptdate",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when statistics estimation supports this case
    @Test(enabled = false)
    void testLineitemQ6()
    {
        statisticsAssertion.check(
                "SELECT * FROM lineitem" +
                        " WHERE l_shipdate >= DATE '1994-01-01'" +
                        " AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR" +
                        " AND l_discount BETWEEN .06 - 0.01 AND .06 + 0.01" +
                        " AND l_quantity < 24",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testLineitemQ7()
    {
        statisticsAssertion.check(
                "SELECT * FROM lineitem WHERE l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testNationQ7()
    {
        statisticsAssertion.check(
                "SELECT n_name FROM nation WHERE n_name = 'FRANCE' OR n_name = 'GERMANY'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when there is support for LIKE in statistics estimation
    @Test(enabled = false)
    void testPartQ9()
    {
        statisticsAssertion.check(
                "SELECT p_name FROM part WHERE p_name LIKE '%green%'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when statistics estimation supports this case
    @Test(enabled = false)
    void testLineitemQ10()
    {
        statisticsAssertion.check(
                "SELECT * FROM lineitem WHERE l_returnflag = 'R'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testOrdersQ13()
    {
        statisticsAssertion.check(
                "SELECT * FROM orders WHERE o_comment NOT LIKE '%special%requests%'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testPartQ16()
    {
        statisticsAssertion.check(
                "SELECT * FROM part" +
                        " WHERE p_brand <> 'Brand#45'" +
                        " AND p_type NOT LIKE 'MEDIUM POLISHED%'" +
                        " AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when statistics estimation supports this case
    @Test(enabled = false)
    void testSupplierQ16()
    {
        statisticsAssertion.check(
                "SELECT * FROM supplier WHERE s_comment LIKE '%Customer%Complaints%'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    // todo Enable this test when statistics estimation supports this case
    @Test(enabled = false)
    void testCustomerQ22()
    {
        statisticsAssertion.check(
                "SELECT * FROM customer WHERE substr(c_phone,1,2) = '13'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }
}
