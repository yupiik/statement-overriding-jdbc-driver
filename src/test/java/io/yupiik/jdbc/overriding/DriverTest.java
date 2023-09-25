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

import org.h2.Driver;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverTest {
    @Test
    void plainDelegation() throws SQLException {
        try (final var h2 = DriverManager.getConnection("jdbc:h2:mem:plainDelegation", "sa", "")) {
            // seed the db
            seedUsers(h2);

            // using wrapping driver
            try (final var wrapper = DriverManager.getConnection(
                    "jdbc:yupiik:statement-overriding-jdbc-driver:driver=" + Driver.class.getName() + ";url=jdbc:h2:mem:plainDelegation", "sa", "");
                 final var stmt = wrapper.prepareStatement("select id, name from some_users");
                 final var set = stmt.executeQuery()) {
                assertEquals(Map.of("0001", "user 1", "0002", "user 2"), asMap(set));
            }
        }
    }

    @Test
    void rewritePreparedStatement() throws SQLException {
        try (final var h2 = DriverManager.getConnection("jdbc:h2:mem:rewritePreparedStatement", "sa", "")) {
            // seed the db
            seedUsers(h2);

            // using wrapping driver
            try (final var wrapper = DriverManager.getConnection(
                    "jdbc:yupiik:statement-overriding-jdbc-driver:driver=" + Driver.class.getName() + ";url=jdbc:h2:mem:rewritePreparedStatement;configuration=DriverTest.properties", "sa", "");
                 final var stmt = wrapper.prepareStatement("select id, name from some_users");
                 final var set = stmt.executeQuery()) {
                assertEquals(Map.of("0002", "user 2"), asMap(set));
            }
        }
    }

    @Test
    void rewritePreparedStatementIgnoreCase() throws SQLException {
        try (final var h2 = DriverManager.getConnection("jdbc:h2:mem:rewritePreparedStatement", "sa", "")) {
            // seed the db
            seedUsers(h2);

            // using wrapping driver
            try (final var wrapper = DriverManager.getConnection(
                    "jdbc:yupiik:statement-overriding-jdbc-driver:driver=" + Driver.class.getName() + ";url=jdbc:h2:mem:rewritePreparedStatement;configuration=DriverTest.properties", "sa", "");
                 final var stmt = wrapper.prepareStatement("SELECT id, name FROM some_users");
                 final var set = stmt.executeQuery()) {
                assertEquals(Map.of("0002", "user 2"), asMap(set));
            }
        }
    }

    @Test
    void rewritePreparedStatementWithBinding() throws SQLException {
        try (final var h2 = DriverManager.getConnection("jdbc:h2:mem:rewritePreparedStatement", "sa", "")) {
            // seed the db
            try (final var stmt = h2.createStatement()) {
                stmt.execute("create table some_users(id varchar(16), name varchar(255), type varchar(1))");
                stmt.execute("insert into some_users(id, name, type) values('0001', 'user 1', 'A')");
                stmt.execute("insert into some_users(id, name, type) values('0002', 'user 2', 'B')");
                stmt.execute("insert into some_users(id, name, type) values('0003', 'user 3', 'B')");
                stmt.execute("insert into some_users(id, name, type) values('0004', 'user 4', 'B')");
                stmt.execute("insert into some_users(id, name, type) values('0005', 'user 5', 'A')");
            }

            // using wrapping driver
            try (final var wrapper = DriverManager.getConnection(
                    "jdbc:yupiik:statement-overriding-jdbc-driver:driver=" + Driver.class.getName() + ";url=jdbc:h2:mem:rewritePreparedStatement;configuration=DriverTest.properties", "sa", "");
                 final var stmt = wrapper.prepareStatement("select id, name from some_users where id like ? and type = ?")) {
                stmt.setString(1, "000%");
                stmt.setString(2, "B");
                try (final var set = stmt.executeQuery()) {
                    assertEquals(
                            Map.of(
                                    "0002", "user 2",
                                    "0003", "user 3",
                                    "0004", "user 4"),
                            asMap(set));
                }
            }
        }
    }

    @Test
    void rewritePreparedStatementBatch() throws SQLException {
        try (final var h2 = DriverManager.getConnection("jdbc:h2:mem:rewritePreparedStatement", "sa", "")) {
            // seed the db
            try (final var stmt = h2.createStatement()) {
                stmt.execute("create table some_users(id varchar(16), name varchar(255), type varchar(1))");
            }

            // using wrapping driver to do the batch insert
            try (final var wrapper = DriverManager.getConnection(
                    "jdbc:yupiik:statement-overriding-jdbc-driver:driver=" + Driver.class.getName() + ";url=jdbc:h2:mem:rewritePreparedStatement;configuration=DriverTest.properties", "sa", "");
                 final var stmt = wrapper.prepareStatement("insert into some_users(id, name, type) values(?, ?, ?)")) {
                for (int i = 1; i <= 5; i++) {
                    stmt.setString(1, "000" + i);
                    stmt.setString(2, "user " + i);
                    stmt.setString(3, i % 4 == 1 ? "A" : "B");
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }

            // assert batch bindings worked
            try (final var assertStmt = h2.createStatement();
                 final var set = assertStmt.executeQuery("select id, name from some_users")) {
                assertEquals(
                        Map.of(
                                "0001", "user 1",
                                "0002", "user 2",
                                "0003", "user 3",
                                "0004", "user 4",
                                "0005", "user 5"),
                        asMap(set));
            }
        }
    }

    @Test
    void noopStatement() throws SQLException {
        try (final var h2 = DriverManager.getConnection("jdbc:h2:mem:rewriteStatement", "sa", "")) {
            // seed the db
            seedUsers(h2);

            // using wrapping driver
            try (final var wrapper = DriverManager.getConnection(
                    "jdbc:yupiik:statement-overriding-jdbc-driver:driver=" + Driver.class.getName() + ";url=jdbc:h2:mem:rewriteStatement;configuration=DriverTest.properties", "sa", "");
                 final var stmt = wrapper.createStatement();
                 final var set = stmt.executeQuery("select id, name from some_users")) {
                // no-op since we only support prepared statement
                assertEquals(Map.of("0001", "user 1", "0002", "user 2"), asMap(set));
            }
        }
    }

    private Map<String, String> asMap(final ResultSet set) {
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                new Iterator<String[]>() {
                                    @Override
                                    public boolean hasNext() {
                                        try {
                                            return set.next();
                                        } catch (final SQLException e) {
                                            throw new IllegalStateException(e);
                                        }
                                    }

                                    @Override
                                    public String[] next() {
                                        try {
                                            return new String[]{set.getString(1), set.getString(2)};
                                        } catch (final SQLException e) {
                                            throw new IllegalStateException(e);
                                        }
                                    }
                                },
                                Spliterator.IMMUTABLE),
                        false)
                .collect(toMap(i -> i[0], i -> i[1]));
    }

    private void seedUsers(final Connection h2) throws SQLException {
        try (final var stmt = h2.createStatement()) {
            stmt.execute("create table some_users(id varchar(16), name varchar(255))");
            stmt.execute("insert into some_users(id, name) values('0001', 'user 1')");
            stmt.execute("insert into some_users(id, name) values('0002', 'user 2')");
        }
    }
}
