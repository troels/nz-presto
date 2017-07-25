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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestPushPartialAggregationThroughJoin
{
    @Test
    public void testPushesPartialAggregationThroughJoin()
    {
        new RuleTester().assertThat(new PushPartialAggregationThroughJoin())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("LEFT_EQUI", BIGINT), p.symbol("LEFT_NON_EQUI", BIGINT), p.symbol("LEFT_GROUP_BY", BIGINT), p.symbol("LEFT_AGGR", BIGINT), p.symbol("LEFT_HASH", BIGINT)),
                                        p.values(p.symbol("RIGHT_EQUI", BIGINT), p.symbol("RIGHT_NON_EQUI", BIGINT), p.symbol("RIGHT_GROUP_BY", BIGINT), p.symbol("RIGHT_HASH", BIGINT)),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI", BIGINT), p.symbol("RIGHT_EQUI", BIGINT))),
                                        ImmutableList.of(p.symbol("LEFT_GROUP_BY", BIGINT), p.symbol("LEFT_AGGR", BIGINT), p.symbol("RIGHT_GROUP_BY", BIGINT)),
                                        Optional.of(expression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                        Optional.of(p.symbol("LEFT_HASH", BIGINT)),
                                        Optional.of(p.symbol("RIGHT_HASH", BIGINT))))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(LEFT_AGGR)"), ImmutableList.of(DOUBLE))
                        .addGroupingSet(p.symbol("LEFT_GROUP_BY", BIGINT), p.symbol("RIGHT_GROUP_BY", BIGINT))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                aggregation(
                                        ImmutableList.of(ImmutableList.of("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_HASH")),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH")),
                                values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH"))));
    }
}
