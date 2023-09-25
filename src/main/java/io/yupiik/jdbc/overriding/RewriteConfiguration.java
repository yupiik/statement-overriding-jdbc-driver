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

public record RewriteConfiguration(Map<Sql, RewriteStatement> configurations) {
    public record Sql(String raw, boolean ignoreCase, int hash) {
        @Override
        public boolean equals(final Object obj) {
            return obj == this || (
                    obj instanceof Sql s &&
                            s.ignoreCase() == ignoreCase() &&
                            (ignoreCase ?
                                    raw().equalsIgnoreCase(s.raw()) :
                                    Objects.equals(raw(), s.raw())));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public record RewriteStatement(String replacement, Map<Integer, Integer> bindingIndices) {
    }
}
