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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * For use with /v1/statement when you send prepared statements via that
 */
@Immutable
public class QuerySubmission
{
    private final String query;
    private final Map<String, String> preparedStatements;

    @JsonCreator
    public QuerySubmission(
            @JsonProperty("query") String query,
            @JsonProperty("preparedStatements") Map<String, String> preparedStatements)
    {
        // Make sure neither query nor preparedstatement ends in semicolon
        query = requireNonNull(query, "query is null");
        this.query = query.trim().replaceAll(";+$", "");

        preparedStatements = requireNonNull(preparedStatements, "preparedStatements is null");
        this.preparedStatements = preparedStatements.entrySet()
                .stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> entry.getValue().trim().replaceAll(";+", "")));
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", query)
                .add("preparedStatements", preparedStatements)
                .toString();
    }
}
