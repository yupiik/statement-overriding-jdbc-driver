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
import io.yupiik.jdbc.overriding.delegation.DelegatingPreparedStatement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class RewritingPrepareStatement extends DelegatingPreparedStatement {
    private final RewriteConfiguration.RewriteStatement configuration;

    private final List<SQLRunnable> bindings = new ArrayList<>();

    public RewritingPrepareStatement(final PreparedStatement preparedStatement, final RewriteConfiguration.RewriteStatement configuration) {
        super(preparedStatement);
        this.configuration = configuration;
    }

    private void onAllBound() throws SQLException {
        for (final var it : bindings) {
            it.run();
        }
        bindings.clear();
    }

    @Override
    public void clearParameters() throws SQLException {
        try {
            super.clearParameters();
        } finally {
            bindings.clear();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        onAllBound();
        return super.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        onAllBound();
        super.addBatch();
    }

    @Override
    public int executeUpdate() throws SQLException {
        onAllBound();
        return super.executeUpdate();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        onAllBound();
        return super.executeLargeUpdate();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        onAllBound();
        return super.executeQuery();
    }

    @Override
    public void close() throws SQLException {
        try {
            super.close();
        } finally {
            bindings.clear();
        }
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType) {
        bindings.add(() -> super.setNull(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), sqlType));
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean x) {
        bindings.add(() -> super.setBoolean(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setByte(final int parameterIndex, final byte x) {
        bindings.add(() -> super.setByte(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setShort(final int parameterIndex, final short x) {
        bindings.add(() -> super.setShort(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setInt(final int parameterIndex, final int x) {
        bindings.add(() -> super.setInt(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setLong(final int parameterIndex, final long x) {
        bindings.add(() -> super.setLong(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setFloat(final int parameterIndex, final float x) {
        bindings.add(() -> super.setFloat(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setDouble(final int parameterIndex, final double x) {
        bindings.add(() -> super.setDouble(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) {
        bindings.add(() -> super.setBigDecimal(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setString(final int parameterIndex, final String x) {
        bindings.add(() -> super.setString(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] x) {
        bindings.add(() -> super.setBytes(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setDate(final int parameterIndex, final Date x) {
        bindings.add(() -> super.setDate(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setTime(final int parameterIndex, final Time x) {
        bindings.add(() -> super.setTime(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) {
        bindings.add(() -> super.setTimestamp(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) {
        bindings.add(() -> super.setAsciiStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) {
        bindings.add(() -> super.setUnicodeStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, length));
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) {
        bindings.add(() -> super.setBinaryStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, length));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) {
        bindings.add(() -> super.setObject(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, targetSqlType));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x) {
        bindings.add(() -> super.setObject(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length) {
        bindings.add(() -> super.setCharacterStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), reader, length));
    }

    @Override
    public void setRef(final int parameterIndex, final Ref x) {
        bindings.add(() -> super.setRef(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob x) {
        bindings.add(() -> super.setBlob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setClob(final int parameterIndex, final Clob x) {
        bindings.add(() -> super.setClob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setArray(final int parameterIndex, final Array x) {
        bindings.add(() -> super.setArray(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) {
        bindings.add(() -> super.setDate(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, cal));
    }

    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) {
        bindings.add(() -> super.setTime(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, cal));
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) {
        bindings.add(() -> super.setTimestamp(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, cal));
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) {
        bindings.add(() -> super.setNull(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), sqlType, typeName));
    }

    @Override
    public void setURL(final int parameterIndex, final URL x) {
        bindings.add(() -> super.setURL(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setRowId(final int parameterIndex, final RowId x) {
        bindings.add(() -> super.setRowId(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setNString(final int parameterIndex, final String value) {
        bindings.add(() -> super.setNString(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), value));
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value, final long length) {
        bindings.add(() -> super.setNCharacterStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), Reader.nullReader(), length));
    }

    @Override
    public void setNClob(final int parameterIndex, final NClob value) {
        bindings.add(() -> super.setNClob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), value));
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader, final long length) {
        bindings.add(() -> super.setClob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), reader, length));
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream, final long length) {
        bindings.add(() -> super.setBlob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), inputStream, length));
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader, final long length) {
        bindings.add(() -> super.setNClob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), reader, length));
    }

    @Override
    public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) {
        bindings.add(() -> super.setSQLXML(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), xmlObject));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength) {
        bindings.add(() -> super.setObject(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) {
        bindings.add(() -> super.setAsciiStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, length));
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) {
        bindings.add(() -> super.setBinaryStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, length));
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final long length) {
        bindings.add(() -> super.setCharacterStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), reader, length));
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x) {
        bindings.add(() -> super.setAsciiStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x) {
        bindings.add(() -> super.setBinaryStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x));
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader) {
        bindings.add(() -> super.setCharacterStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), reader));
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value) {
        bindings.add(() -> super.setNCharacterStream(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), value));
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader) {
        bindings.add(() -> super.setClob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), reader));
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream) {
        bindings.add(() -> super.setBlob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), inputStream));
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader) {
        bindings.add(() -> super.setNClob(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), reader));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType, final int scaleOrLength) {
        bindings.add(() -> super.setObject(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType) {
        bindings.add(() -> super.setObject(configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex), x, targetSqlType));
    }

    @FunctionalInterface
    private interface SQLRunnable {
        void run() throws SQLException;
    }
}
