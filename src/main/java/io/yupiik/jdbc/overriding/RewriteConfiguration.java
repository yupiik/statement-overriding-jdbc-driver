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

import java.util.Map;
import java.util.Objects;

public class RewriteConfiguration {
    private final Map<Sql, RewriteStatement> configurations;

    public RewriteConfiguration(final Map<Sql, RewriteStatement> configurations) {
        this.configurations = configurations;
    }

    public Map<Sql, RewriteStatement> configurations() {
        return configurations;
    }

    public static class Sql {
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
    }

    public static class RewriteStatement {
        private final String replacement;
        private final Map<Integer, Integer> bindingIndices;

        public RewriteStatement(final String replacement, final Map<Integer, Integer> bindingIndices) {
            this.replacement = replacement;
            this.bindingIndices = bindingIndices;
        }

        public String replacement() {
            return replacement;
        }

        public Map<Integer, Integer> bindingIndices() {
            return bindingIndices;
        }
    }
}
