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
package io.yupiik.jdbc.overriding.rewrite;

import io.yupiik.jdbc.overriding.RewriteConfiguration;
import io.yupiik.jdbc.overriding.delegation.DelegatingConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RewritingConnection extends DelegatingConnection {
    private final RewriteConfiguration configuration;

    public RewritingConnection(final Connection delegate, final RewriteConfiguration configuration) {
        super(delegate);
        this.configuration = configuration;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        final var rewriteStatement = sql == null ? null : configuration.configurations().get(sql.strip());
        if (rewriteStatement == null) {
            return super.prepareStatement(sql);
        }
        return new RewritingPrepareStatement(super.prepareStatement(rewriteStatement.replacement()), rewriteStatement);
    }
}