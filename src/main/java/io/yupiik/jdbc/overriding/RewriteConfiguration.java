/*
 * Copyright (c) 2023 - Yupiik SAS - https://www.yupiik.com
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.yupiik.jdbc.overriding;

import io.yupiik.jdbc.overriding.rewrite.MatchedRewriting;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.yupiik.jdbc.overriding.RewriteConfiguration.RewriteType.REGEX;
import static java.util.stream.Collectors.toMap;

public class RewriteConfiguration {
    private final Map<Sql, RewriteStatement> configurations;
    private final Map<Pattern, RewriteStatement> regexes;

    public RewriteConfiguration(final Map<Sql, RewriteStatement> configurations) {
        this.configurations = configurations;
        this.regexes = configurations.entrySet().stream()
                .filter(i -> i.getValue().type() == REGEX)
                .collect(toMap(i -> Pattern.compile(i.getKey().raw()), Map.Entry::getValue));
    }

    public Map<Sql, RewriteStatement> configurations() {
        return configurations;
    }

    public MatchedRewriting tryRewriteUsingRegexes(final String sql) {
        if (regexes.isEmpty()) {
            return null;
        }

        for (final var entry : regexes.entrySet()) {
            final var matcher = entry.getKey().matcher(sql);
            if (matcher.matches()) {
                return new MatchedRewriting(matcher.replaceFirst(entry.getValue().replacement()), entry.getValue());
            }
        }
        return null;
    }

    public static class Sql implements Predicate<Sql> {
        private final String raw;
        private final boolean ignoreCase;
        private final int hash;

        public Sql(final String raw, final boolean ignoreCase, final int hash) {
            this.raw = raw;
            this.ignoreCase = ignoreCase;
            this.hash = hash;
        }

        public String raw() {
            return raw;
        }

        public boolean ignoreCase() {
            return ignoreCase;
        }

        @Override
        public boolean equals(final Object obj) {
            return obj == this || (obj instanceof Sql && isEquals((Sql) obj));
        }

        @Override
        public int hashCode() {
            return hash;
        }

        private boolean isEquals(final Sql s) {
            return s.ignoreCase == ignoreCase &&
                    (ignoreCase ?
                            raw.equalsIgnoreCase(s.raw) :
                            Objects.equals(raw, s.raw));
        }

        @Override
        public boolean test(final Sql sql) {
            return equals(sql);
        }
    }

    public static class RewriteStatement {
        private final String replacement;
        private final Map<Integer, Integer> bindingIndices;
        private final Map<Integer, Integer> resultSetIndexOverride;
        private final Map<String, String> resultSetNameOverride;
        private final RewriteType type;

        public RewriteStatement(final String replacement, final Map<Integer, Integer> bindingIndices, final RewriteType type,
                                final Map<Integer, Integer> resultSetIndexOverride, final Map<String, String> resultSetNameOverride) {
            this.replacement = replacement;
            this.bindingIndices = bindingIndices;
            this.resultSetIndexOverride = resultSetIndexOverride;
            this.resultSetNameOverride = resultSetNameOverride;
            this.type = type;
        }

        public RewriteType type() {
            return type;
        }

        public Map<Integer, Integer> resultSetIndexOverride() {
            return resultSetIndexOverride;
        }

        public Map<String, String> resultSetNameOverride() {
            return resultSetNameOverride;
        }

        public Map<Integer, Integer> bindingIndices() {
            return bindingIndices;
        }

        public String replacement() {
            return replacement;
        }
    }

    public enum RewriteType {
        PLAIN, REGEX
    }
}
