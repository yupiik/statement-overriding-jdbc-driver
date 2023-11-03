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

    public RewritingPrepareStatement(final PreparedStatement preparedStatement,
                                     final RewriteConfiguration.RewriteStatement configuration) {
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
        final var resultSet = super.executeQuery();
        if (configuration.resultSetIndexOverride() != null || configuration.resultSetNameOverride() != null) {
            return new RemappingResultSet(resultSet, configuration.resultSetIndexOverride(), configuration.resultSetNameOverride());
        }
        return resultSet;
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
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNull(index, sqlType));
        }
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBoolean(index, x));
        }
    }

    @Override
    public void setByte(final int parameterIndex, final byte x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setByte(index, x));
        }
    }

    @Override
    public void setShort(final int parameterIndex, final short x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setShort(index, x));
        }
    }

    @Override
    public void setInt(final int parameterIndex, final int x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setInt(index, x));
        }
    }

    @Override
    public void setLong(final int parameterIndex, final long x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setLong(index, x));
        }
    }

    @Override
    public void setFloat(final int parameterIndex, final float x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setFloat(index, x));
        }
    }

    @Override
    public void setDouble(final int parameterIndex, final double x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setDouble(index, x));
        }
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBigDecimal(index, x));
        }
    }

    @Override
    public void setString(final int parameterIndex, final String x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setString(index, x));
        }
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBytes(index, x));
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setDate(index, x));
        }
    }

    @Override
    public void setTime(final int parameterIndex, final Time x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setTime(index, x));
        }
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setTimestamp(index, x));
        }
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setAsciiStream(index, x));
        }
    }

    @Override
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setUnicodeStream(index, x, length));
        }
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBinaryStream(index, x, length));
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setObject(index, x, targetSqlType));
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setObject(index, x));
        }
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setCharacterStream(index, reader, length));
        }
    }

    @Override
    public void setRef(final int parameterIndex, final Ref x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setRef(index, x));
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBlob(index, x));
        }
    }

    @Override
    public void setClob(final int parameterIndex, final Clob x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setClob(index, x));
        }
    }

    @Override
    public void setArray(final int parameterIndex, final Array x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setArray(index, x));
        }
    }

    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setDate(index, x, cal));
        }
    }

    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setTime(index, x, cal));
        }
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setTimestamp(index, x, cal));
        }
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNull(index, sqlType, typeName));
        }
    }

    @Override
    public void setURL(final int parameterIndex, final URL x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setURL(index, x));
        }
    }

    @Override
    public void setRowId(final int parameterIndex, final RowId x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setRowId(index, x));
        }
    }

    @Override
    public void setNString(final int parameterIndex, final String value) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNString(index, value));
        }
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value, final long length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNCharacterStream(index, Reader.nullReader(), length));
        }
    }

    @Override
    public void setNClob(final int parameterIndex, final NClob value) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNClob(index, value));
        }
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader, final long length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setClob(index, reader, length));
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream, final long length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBlob(index, inputStream, length));
        }
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader, final long length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNClob(index, reader, length));
        }
    }

    @Override
    public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setSQLXML(index, xmlObject));
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setObject(index, x, targetSqlType, scaleOrLength));
        }
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setAsciiStream(index, x, length));
        }
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBinaryStream(index, x, length));
        }
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final long length) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setCharacterStream(index, reader, length));
        }
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setAsciiStream(index, x));
        }
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBinaryStream(index, x));
        }
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setCharacterStream(index, reader));
        }
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNCharacterStream(index, value));
        }
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setClob(index, reader));
        }
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setBlob(index, inputStream));
        }
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setNClob(index, reader));
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType, final int scaleOrLength) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setObject(index, x, targetSqlType, scaleOrLength));
        }
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType) {
        final var index = configuration.bindingIndices().getOrDefault(parameterIndex, parameterIndex);
        if (index > 0) {
            bindings.add(() -> super.setObject(index, x, targetSqlType));
        }
    }

    @FunctionalInterface
    private interface SQLRunnable {
        void run() throws SQLException;
    }
}
