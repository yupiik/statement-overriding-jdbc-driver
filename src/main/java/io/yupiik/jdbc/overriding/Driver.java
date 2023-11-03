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

import io.yupiik.jdbc.overriding.rewrite.RewritingConnection;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Driver implements java.sql.Driver {
    private static final Pattern ARG_SPLITTER = Pattern.compile(";");
    private static final Logger PARENT_LOGGER = Logger.getLogger("io.yupiik.jdbc");
    private static final String SELF_PREFIX = "jdbc:yupiik:statement-overriding-jdbc-driver:";

    // default to false for now like pg for ex
    private static final boolean JDBC_COMPLIANT = Boolean.getBoolean(Driver.class.getName() + ".jdbcCompliant");

    private static final Map<String, WeakReference<UrlData>> CACHE = new ConcurrentHashMap<>();

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (final SQLException e) {
            PARENT_LOGGER.log(SEVERE, e, e::getMessage);
        }
    }

    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        final var cached = CACHE.get(url);
        if (cached != null) {
            final var existing = cached.get();
            if (existing != null) {
                return new RewritingConnection(existing.driver.connect(existing.url, info), existing.configuration);
            }
        }

        final var parsed = parseUrl(url);
        final var delegatingUrl = requireNonNull(parsed.get("url"), "No 'url' set on '" + url + "'");
        try {
            final var loader = ofNullable(Thread.currentThread().getContextClassLoader())
                    .orElseGet(Driver.class::getClassLoader);
            final var urlData = new UrlData(
                    newDriver(delegatingUrl, parsed, loader), delegatingUrl,
                    new RewriteConfiguration(loadConfiguration(loader, parsed.get("configuration"))));
            CACHE.put(url, new WeakReference<>(urlData));
            return new RewritingConnection(urlData.driver.connect(delegatingUrl, info), urlData.configuration);
        } catch (final SQLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean acceptsURL(final String url) {
        return url != null && url.startsWith(SELF_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
        if (!acceptsURL(url)) {
            return null;
        }
        return parseUrl(url).entrySet().stream()
                .map(it -> new DriverPropertyInfo(it.getKey(), it.getValue()))
                .toArray(DriverPropertyInfo[]::new);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return JDBC_COMPLIANT;
    }

    @Override
    public Logger getParentLogger() {
        return PARENT_LOGGER;
    }

    private java.sql.Driver newDriver(final String url,
                                      final Map<String, String> parsed,
                                      final ClassLoader loader) throws SQLException {
        try {
            final var constructor = Class.forName(
                            requireNonNull(parsed.get("driver"), "No 'driver' found on '" + url + "'"),
                            true, loader)
                    .asSubclass(java.sql.Driver.class)
                    .getDeclaredConstructor();
            if (!constructor.canAccess(null)) {
                constructor.setAccessible(true);
            }
            return constructor.newInstance();
        } catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException |
                       IllegalAccessException | InvocationTargetException cnfe) {
            throw new SQLException("Invalid driver: " + cnfe.getMessage(), cnfe);
        }
    }

    private Map<String, String> parseUrl(final String url) {
        final var values = url.substring(SELF_PREFIX.length());
        final var known = Set.of("driver", "configuration", "url", "username", "password");
        return Stream.of(ARG_SPLITTER.split(values))
                .map(it -> {
                    final int sep = it.indexOf('=');
                    return sep > 0 ?
                            new String[]{it.substring(0, sep).strip(), it.substring(sep + 1).strip()} :
                            new String[]{it.strip(), "true"};
                })
                .filter(it -> known.contains(it[0]))
                .collect(toMap(
                        it -> it[0],
                        it -> "url".equals(it[0]) ?
                                it[1].replace("$semicolon", ";") :
                                it[1],
                        (a, b) -> b));
    }

    private Map<RewriteConfiguration.Sql, RewriteConfiguration.RewriteStatement> loadConfiguration(
            final ClassLoader loader, final String configuration) {
        if (configuration == null || configuration.isBlank()) {
            return Map.of();
        }

        final var props = new Properties();
        final var location = Path.of(configuration);
        if (Files.exists(location)) {
            PARENT_LOGGER.info(() -> "Loading '" + location.toAbsolutePath().normalize() + "'");
            try (final var is = Files.newInputStream(location)) {
                props.load(is);
            } catch (final IOException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            try (final var is = loader.getResourceAsStream(configuration)) {
                if (is != null) {
                    props.load(is);
                }
            } catch (final IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        final var suffix = ".sql.matching";
        final var keys = props.stringPropertyNames().stream()
                .filter(k -> k.endsWith(suffix))
                .map(String::strip)
                .collect(toList());
        return keys.stream()
                .collect(toMap(
                        key -> {
                            final var sql = props.getProperty(key);
                            final var ignoreCase = Boolean.parseBoolean(props.getProperty(key.substring(0, key.length() - suffix.length()), "true"));
                            return new RewriteConfiguration.Sql(
                                    sql, ignoreCase,
                                    ignoreCase ? sql.toLowerCase(ROOT).hashCode() : sql.hashCode());
                        },
                        key -> {
                            final var prefix = key.substring(0, key.length() - suffix.length());
                            final var bindingsPrefix = prefix + ".bindings.";
                            final var resultSetPrefix = prefix + ".resultset.";
                            final var resultSetIndicesPrefix = resultSetPrefix + "index.";
                            final var resultSetNamesPrefix = resultSetPrefix + "name.";
                            return new RewriteConfiguration.RewriteStatement(
                                    props.getProperty(prefix + ".sql.replacing", props.getProperty(key)).strip(),
                                    props.stringPropertyNames().stream()
                                            .filter(b -> b.startsWith(bindingsPrefix))
                                            .collect(toMap(i -> Integer.parseInt(i.substring(bindingsPrefix.length()).strip()), i -> Integer.parseInt(props.getProperty(i).strip()))),
                                    props.stringPropertyNames().stream()
                                            .filter(b -> b.startsWith(resultSetIndicesPrefix))
                                            .collect(toMap(i -> Integer.parseInt(i.substring(resultSetIndicesPrefix.length()).strip()), i -> Integer.parseInt(props.getProperty(i).strip()))),
                                    props.stringPropertyNames().stream()
                                            .filter(b -> b.startsWith(resultSetNamesPrefix))
                                            .collect(toMap(i -> i.substring(resultSetNamesPrefix.length()).strip(), i -> props.getProperty(i).strip())));
                        }));
    }

    private static class UrlData {
        private final java.sql.Driver driver;
        private final String url;
        private final RewriteConfiguration configuration;

        private UrlData(final java.sql.Driver driver, final String url, final RewriteConfiguration configuration) {
            this.driver = driver;
            this.url = url;
            this.configuration = configuration;
        }
    }
}
