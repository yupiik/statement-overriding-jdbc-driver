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
package io.yupiik.jdbc.overriding.delegation;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class DelegatingPreparedStatement implements PreparedStatement {
    private final PreparedStatement preparedStatement;

    public DelegatingPreparedStatement(final PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return preparedStatement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return preparedStatement.executeUpdate();
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        preparedStatement.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
        preparedStatement.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        preparedStatement.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        preparedStatement.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {
        preparedStatement.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        preparedStatement.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        preparedStatement.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        preparedStatement.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        preparedStatement.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {
        preparedStatement.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        preparedStatement.setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(final int parameterIndex, final Date x) throws SQLException {
        preparedStatement.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        preparedStatement.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        preparedStatement.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        preparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        preparedStatement.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        preparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        preparedStatement.clearParameters();
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
        preparedStatement.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        preparedStatement.setObject(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        return preparedStatement.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        preparedStatement.addBatch();
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length) throws SQLException {
        preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(final int parameterIndex, final Ref x) throws SQLException {
        preparedStatement.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
        preparedStatement.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(final int parameterIndex, final Clob x) throws SQLException {
        preparedStatement.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(final int parameterIndex, final Array x) throws SQLException {
        preparedStatement.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return preparedStatement.getMetaData();
    }

    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException {
        preparedStatement.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException {
        preparedStatement.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
        preparedStatement.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
        preparedStatement.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(final int parameterIndex, final URL x) throws SQLException {
        preparedStatement.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return preparedStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
        preparedStatement.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(final int parameterIndex, final String value) throws SQLException {
        preparedStatement.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value, final long length) throws SQLException {
        preparedStatement.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(final int parameterIndex, final NClob value) throws SQLException {
        preparedStatement.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader, final long length) throws SQLException {
        preparedStatement.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream, final long length) throws SQLException {
        preparedStatement.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader, final long length) throws SQLException {
        preparedStatement.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) throws SQLException {
        preparedStatement.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength) throws SQLException {
        preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
        preparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
        preparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final long length) throws SQLException {
        preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException {
        preparedStatement.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException {
        preparedStatement.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader) throws SQLException {
        preparedStatement.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value) throws SQLException {
        preparedStatement.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader) throws SQLException {
        preparedStatement.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException {
        preparedStatement.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader) throws SQLException {
        preparedStatement.setNClob(parameterIndex, reader);
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType, final int scaleOrLength) throws SQLException {
        preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final SQLType targetSqlType) throws SQLException {
        preparedStatement.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return preparedStatement.executeLargeUpdate();
    }

    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        return preparedStatement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(final String sql) throws SQLException {
        return preparedStatement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        preparedStatement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return preparedStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(final int max) throws SQLException {
        preparedStatement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return preparedStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(final int max) throws SQLException {
        preparedStatement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {
        preparedStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return preparedStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        preparedStatement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        preparedStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return preparedStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        preparedStatement.clearWarnings();
    }

    @Override
    public void setCursorName(final String name) throws SQLException {
        preparedStatement.setCursorName(name);
    }

    @Override
    public boolean execute(final String sql) throws SQLException {
        return preparedStatement.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return preparedStatement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return preparedStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return preparedStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        preparedStatement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return preparedStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        preparedStatement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return preparedStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return preparedStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return preparedStatement.getResultSetType();
    }

    @Override
    public void addBatch(final String sql) throws SQLException {
        preparedStatement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        preparedStatement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return preparedStatement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return preparedStatement.getConnection();
    }

    @Override
    public boolean getMoreResults(final int current) throws SQLException {
        return preparedStatement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return preparedStatement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        return preparedStatement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        return preparedStatement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        return preparedStatement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        return preparedStatement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
        return preparedStatement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        return preparedStatement.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return preparedStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return preparedStatement.isClosed();
    }

    @Override
    public void setPoolable(final boolean poolable) throws SQLException {
        preparedStatement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return preparedStatement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        preparedStatement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return preparedStatement.isCloseOnCompletion();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return preparedStatement.getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(final long max) throws SQLException {
        preparedStatement.setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return preparedStatement.getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return preparedStatement.executeLargeBatch();
    }

    @Override
    public long executeLargeUpdate(final String sql) throws SQLException {
        return preparedStatement.executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        return preparedStatement.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        return preparedStatement.executeLargeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(final String sql, final String[] columnNames) throws SQLException {
        return preparedStatement.executeLargeUpdate(sql, columnNames);
    }

    @Override
    public String enquoteLiteral(final String val) throws SQLException {
        return preparedStatement.enquoteLiteral(val);
    }

    @Override
    public String enquoteIdentifier(final String identifier, final boolean alwaysQuote) throws SQLException {
        return preparedStatement.enquoteIdentifier(identifier, alwaysQuote);
    }

    @Override
    public boolean isSimpleIdentifier(final String identifier) throws SQLException {
        return preparedStatement.isSimpleIdentifier(identifier);
    }

    @Override
    public String enquoteNCharLiteral(final String val) throws SQLException {
        return preparedStatement.enquoteNCharLiteral(val);
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return DelegatingPreparedStatement.class == iface ? iface.cast(this) : preparedStatement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return DelegatingPreparedStatement.class == iface || preparedStatement.isWrapperFor(iface);
    }
}
