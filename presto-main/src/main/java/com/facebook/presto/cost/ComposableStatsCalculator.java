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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Matchable;
import com.facebook.presto.matching.MatchingEngine;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ComposableStatsCalculator
        implements StatsCalculator
{
    private final MatchingEngine<Rule> rules;
    private final List<Normalizer> normalizers;

    public ComposableStatsCalculator(Set<Rule> rules, List<Normalizer> normalizers)
    {
        this.rules = MatchingEngine.<Rule>builder()
                .register(rules)
                .build();
        this.normalizers = ImmutableList.copyOf(normalizers);
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(lookup, session, types);
        return planNode.accept(visitor, null);
    }

    public interface Rule extends Matchable
    {
        Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types);
    }

    public interface Normalizer
    {
        PlanNodeStatsEstimate normalize(PlanNode node, PlanNodeStatsEstimate estimate, Map<Symbol, Type> types);
    }

    private class Visitor
            extends PlanVisitor<PlanNodeStatsEstimate, Void>
    {
        private final Lookup lookup;
        private final Session session;
        private final Map<Symbol, Type> types;

        public Visitor(Lookup lookup, Session session, Map<Symbol, Type> types)
        {
            this.lookup = lookup;
            this.session = session;
            this.types = ImmutableMap.copyOf(types);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, Void context)
        {
            Iterator<Rule> ruleIterator = rules.getCandidates(node).iterator();
            while (ruleIterator.hasNext()) {
                Rule rule = ruleIterator.next();
                Optional<PlanNodeStatsEstimate> calculatedStats = rule.calculate(node, lookup, session, types);
                if (calculatedStats.isPresent()) {
                    return normalize(node, calculatedStats.get());
                }
            }
            return PlanNodeStatsEstimate.UNKNOWN_STATS;
        }

        private PlanNodeStatsEstimate normalize(PlanNode node, PlanNodeStatsEstimate estimate)
        {
            for (Normalizer normalizer : normalizers) {
                estimate = normalizer.normalize(node, estimate, types);
            }
            return estimate;
        }
    }
}
