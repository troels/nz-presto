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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

public class MergeLimits
    implements Rule
{
    private static final Pattern PATTERN = Pattern.matchByClass(LimitNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        LimitNode parent = (LimitNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof LimitNode)) {
            return Optional.empty();
        }

        LimitNode child = (LimitNode) source;

        return Optional.of(
                new LimitNode(
                        parent.getId(),
                        child.getSource(),
                        Math.min(parent.getCount(), child.getCount()),
                        parent.isPartial()));
    }
}
