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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class RemappingResultSet implements ResultSet {
    private final ResultSet delegate;
    private final Map<Integer, Integer> indexMapping;
    private final Map<String, String> namesMapping;

    public RemappingResultSet(final ResultSet resultSet,
                              final Map<Integer, Integer> indicesMapping,
                              final Map<String, String> namesMapping) {
        this.delegate = resultSet;
        this.indexMapping = indicesMapping;
        this.namesMapping = namesMapping;
    }

    private int index(final int idx) {
        return indexMapping == null ? idx : indexMapping.getOrDefault(idx, idx);
    }

    private String name(final String name) {
        return namesMapping == null ? name : namesMapping.getOrDefault(name, name);
    }

    @Override
    public boolean next() throws SQLException {
        return delegate.next();
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        return delegate.getString(index(index(columnIndex)));
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        return delegate.getBoolean(index(columnIndex));
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        return delegate.getByte(index(columnIndex));
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        return delegate.getShort(index(columnIndex));
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        return delegate.getInt(index(columnIndex));
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        return delegate.getLong(index(columnIndex));
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        return delegate.getFloat(index(columnIndex));
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        return delegate.getDouble(index(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        return delegate.getBigDecimal(index(columnIndex), scale);
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        return delegate.getBytes(index(columnIndex));
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        return delegate.getDate(index(columnIndex));
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        return delegate.getTime(index(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        return delegate.getTimestamp(index(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        return delegate.getAsciiStream(index(columnIndex));
    }

    @Override
    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
        return delegate.getUnicodeStream(index(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        return delegate.getBinaryStream(index(columnIndex));
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        return delegate.getString(name(columnLabel));
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        return delegate.getBoolean(name(columnLabel));
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        return delegate.getByte(name(columnLabel));
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        return delegate.getShort(name(columnLabel));
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        return delegate.getInt(name(columnLabel));
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        return delegate.getLong(name(columnLabel));
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        return delegate.getFloat(name(columnLabel));
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        return delegate.getDouble(name(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        return delegate.getBigDecimal(name(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        return delegate.getBytes(name(columnLabel));
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        return delegate.getDate(name(columnLabel));
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        return delegate.getTime(name(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        return delegate.getTimestamp(name(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        return delegate.getAsciiStream(name(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
        return delegate.getUnicodeStream(name(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        return delegate.getBinaryStream(name(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return delegate.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return delegate.getMetaData();
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        return delegate.getObject(index(columnIndex));
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        return delegate.getObject(name(columnLabel));
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        return delegate.findColumn(name(columnLabel));
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        return delegate.getCharacterStream(index(columnIndex));
    }

    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        return delegate.getCharacterStream(name(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        return delegate.getBigDecimal(index(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        return delegate.getBigDecimal(name(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return delegate.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return delegate.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return delegate.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return delegate.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        delegate.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        delegate.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return delegate.first();
    }

    @Override
    public boolean last() throws SQLException {
        return delegate.last();
    }

    @Override
    public int getRow() throws SQLException {
        return delegate.getRow();
    }

    @Override
    public boolean absolute(final int row) throws SQLException {
        return delegate.absolute(row);
    }

    @Override
    public boolean relative(final int rows) throws SQLException {
        return delegate.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return delegate.previous();
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        delegate.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return delegate.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return delegate.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return delegate.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return delegate.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return delegate.rowDeleted();
    }

    @Override
    public void updateNull(final int columnIndex) throws SQLException {
        delegate.updateNull(index(columnIndex));
    }

    @Override
    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
        delegate.updateBoolean(index(columnIndex), x);
    }

    @Override
    public void updateByte(final int columnIndex, final byte x) throws SQLException {
        delegate.updateByte(index(columnIndex), x);
    }

    @Override
    public void updateShort(final int columnIndex, final short x) throws SQLException {
        delegate.updateShort(index(columnIndex), x);
    }

    @Override
    public void updateInt(final int columnIndex, final int x) throws SQLException {
        delegate.updateInt(index(columnIndex), x);
    }

    @Override
    public void updateLong(final int columnIndex, final long x) throws SQLException {
        delegate.updateLong(index(columnIndex), x);
    }

    @Override
    public void updateFloat(final int columnIndex, final float x) throws SQLException {
        delegate.updateFloat(index(columnIndex), x);
    }

    @Override
    public void updateDouble(final int columnIndex, final double x) throws SQLException {
        delegate.updateDouble(index(columnIndex), x);
    }

    @Override
    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
        delegate.updateBigDecimal(index(columnIndex), x);
    }

    @Override
    public void updateString(final int columnIndex, final String x) throws SQLException {
        delegate.updateString(index(columnIndex), x);
    }

    @Override
    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
        delegate.updateBytes(index(columnIndex), x);
    }

    @Override
    public void updateDate(final int columnIndex, final Date x) throws SQLException {
        delegate.updateDate(index(columnIndex), x);
    }

    @Override
    public void updateTime(final int columnIndex, final Time x) throws SQLException {
        delegate.updateTime(index(columnIndex), x);
    }

    @Override
    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
        delegate.updateTimestamp(index(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
        delegate.updateAsciiStream(index(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
        delegate.updateBinaryStream(index(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {
        delegate.updateCharacterStream(index(columnIndex), x, length);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException {
        delegate.updateObject(index(columnIndex), x, scaleOrLength);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x) throws SQLException {
        delegate.updateObject(index(columnIndex), x);
    }

    @Override
    public void updateNull(final String columnLabel) throws SQLException {
        delegate.updateNull(name(columnLabel));
    }

    @Override
    public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
        delegate.updateBoolean(name(columnLabel), x);
    }

    @Override
    public void updateByte(final String columnLabel, final byte x) throws SQLException {
        delegate.updateByte(name(columnLabel), x);
    }

    @Override
    public void updateShort(final String columnLabel, final short x) throws SQLException {
        delegate.updateShort(name(columnLabel), x);
    }

    @Override
    public void updateInt(final String columnLabel, final int x) throws SQLException {
        delegate.updateInt(name(columnLabel), x);
    }

    @Override
    public void updateLong(final String columnLabel, final long x) throws SQLException {
        delegate.updateLong(name(columnLabel), x);
    }

    @Override
    public void updateFloat(final String columnLabel, final float x) throws SQLException {
        delegate.updateFloat(name(columnLabel), x);
    }

    @Override
    public void updateDouble(final String columnLabel, final double x) throws SQLException {
        delegate.updateDouble(name(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
        delegate.updateBigDecimal(name(columnLabel), x);
    }

    @Override
    public void updateString(final String columnLabel, final String x) throws SQLException {
        delegate.updateString(name(columnLabel), x);
    }

    @Override
    public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
        delegate.updateBytes(name(columnLabel), x);
    }

    @Override
    public void updateDate(final String columnLabel, final Date x) throws SQLException {
        delegate.updateDate(name(columnLabel), x);
    }

    @Override
    public void updateTime(final String columnLabel, final Time x) throws SQLException {
        delegate.updateTime(name(columnLabel), x);
    }

    @Override
    public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
        delegate.updateTimestamp(name(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
        delegate.updateAsciiStream(name(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
        delegate.updateBinaryStream(name(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader, final int length) throws SQLException {
        delegate.updateCharacterStream(name(columnLabel), reader, length);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x, final int scaleOrLength) throws SQLException {
        delegate.updateObject(name(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x) throws SQLException {
        delegate.updateObject(name(columnLabel), x);
    }

    @Override
    public void insertRow() throws SQLException {
        delegate.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        delegate.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        delegate.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        delegate.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        delegate.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        delegate.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        delegate.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return delegate.getStatement();
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        return delegate.getObject(index(columnIndex), map);
    }

    @Override
    public Ref getRef(final int columnIndex) throws SQLException {
        return delegate.getRef(index(columnIndex));
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        return delegate.getBlob(index(columnIndex));
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        return delegate.getClob(index(columnIndex));
    }

    @Override
    public Array getArray(final int columnIndex) throws SQLException {
        return delegate.getArray(index(columnIndex));
    }

    @Override
    public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException {
        return delegate.getObject(name(columnLabel), map);
    }

    @Override
    public Ref getRef(final String columnLabel) throws SQLException {
        return delegate.getRef(name(columnLabel));
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        return delegate.getBlob(name(columnLabel));
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        return delegate.getClob(name(columnLabel));
    }

    @Override
    public Array getArray(final String columnLabel) throws SQLException {
        return delegate.getArray(name(columnLabel));
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        return delegate.getDate(index(columnIndex), cal);
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        return delegate.getDate(name(columnLabel), cal);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        return delegate.getTime(index(columnIndex), cal);
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
        return delegate.getTime(name(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        return delegate.getTimestamp(index(columnIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
        return delegate.getTimestamp(name(columnLabel), cal);
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        return delegate.getURL(index(columnIndex));
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        return delegate.getURL(name(columnLabel));
    }

    @Override
    public void updateRef(final int columnIndex, final Ref x) throws SQLException {
        delegate.updateRef(index(columnIndex), x);
    }

    @Override
    public void updateRef(final String columnLabel, final Ref x) throws SQLException {
        delegate.updateRef(name(columnLabel), x);
    }

    @Override
    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
        delegate.updateBlob(index(columnIndex), x);
    }

    @Override
    public void updateBlob(final String columnLabel, final Blob x) throws SQLException {
        delegate.updateBlob(name(columnLabel), x);
    }

    @Override
    public void updateClob(final int columnIndex, final Clob x) throws SQLException {
        delegate.updateClob(index(columnIndex), x);
    }

    @Override
    public void updateClob(final String columnLabel, final Clob x) throws SQLException {
        delegate.updateClob(name(columnLabel), x);
    }

    @Override
    public void updateArray(final int columnIndex, final Array x) throws SQLException {
        delegate.updateArray(index(columnIndex), x);
    }

    @Override
    public void updateArray(final String columnLabel, final Array x) throws SQLException {
        delegate.updateArray(name(columnLabel), x);
    }

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {
        return delegate.getRowId(index(columnIndex));
    }

    @Override
    public RowId getRowId(final String columnLabel) throws SQLException {
        return delegate.getRowId(name(columnLabel));
    }

    @Override
    public void updateRowId(final int columnIndex, final RowId x) throws SQLException {
        delegate.updateRowId(index(columnIndex), x);
    }

    @Override
    public void updateRowId(final String columnLabel, final RowId x) throws SQLException {
        delegate.updateRowId(name(columnLabel), x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public void updateNString(final int columnIndex, final String nString) throws SQLException {
        delegate.updateNString(index(columnIndex), nString);
    }

    @Override
    public void updateNString(final String columnLabel, final String nString) throws SQLException {
        delegate.updateNString(name(columnLabel), nString);
    }

    @Override
    public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException {
        delegate.updateNClob(index(columnIndex), nClob);
    }

    @Override
    public void updateNClob(final String columnLabel, final NClob nClob) throws SQLException {
        delegate.updateNClob(name(columnLabel), nClob);
    }

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {
        return delegate.getNClob(index(columnIndex));
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
        return delegate.getNClob(name(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        return delegate.getSQLXML(index(columnIndex));
    }

    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        return delegate.getSQLXML(name(columnLabel));
    }

    @Override
    public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {
        delegate.updateSQLXML(index(columnIndex), xmlObject);
    }

    @Override
    public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException {
        delegate.updateSQLXML(name(columnLabel), xmlObject);
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        return delegate.getNString(index(columnIndex));
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
        return delegate.getNString(name(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
        return delegate.getNCharacterStream(index(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(final String columnLabel) throws SQLException {
        return delegate.getNCharacterStream(name(columnLabel));
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        delegate.updateNCharacterStream(index(columnIndex), x, length);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader reader, final long length) throws SQLException {
        delegate.updateNCharacterStream(name(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        delegate.updateAsciiStream(index(columnIndex), x, length);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        delegate.updateBinaryStream(index(columnIndex), x, length);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        delegate.updateCharacterStream(index(columnIndex), x, length);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x, final long length) throws SQLException {
        delegate.updateAsciiStream(name(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x, final long length) throws SQLException {
        delegate.updateBinaryStream(name(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader, final long length) throws SQLException {
        delegate.updateCharacterStream(name(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream inputStream, final long length) throws SQLException {
        delegate.updateBlob(index(columnIndex), inputStream, length);
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream inputStream, final long length) throws SQLException {
        delegate.updateBlob(name(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
        delegate.updateClob(index(columnIndex), reader, length);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
        delegate.updateClob(name(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
        delegate.updateNClob(index(columnIndex), reader, length);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
        delegate.updateNClob(name(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        delegate.updateNCharacterStream(index(columnIndex), x);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
        delegate.updateNCharacterStream(name(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
        delegate.updateAsciiStream(index(columnIndex), x);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
        delegate.updateBinaryStream(index(columnIndex), x);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        delegate.updateCharacterStream(index(columnIndex), x);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
        delegate.updateAsciiStream(name(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException {
        delegate.updateBinaryStream(name(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
        delegate.updateCharacterStream(name(columnLabel), reader);
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {
        delegate.updateBlob(index(columnIndex), inputStream);
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException {
        delegate.updateBlob(name(columnLabel), inputStream);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader) throws SQLException {
        delegate.updateClob(index(columnIndex), reader);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader) throws SQLException {
        delegate.updateClob(name(columnLabel), reader);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader) throws SQLException {
        delegate.updateNClob(index(columnIndex), reader);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader) throws SQLException {
        delegate.updateNClob(name(columnLabel), reader);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        return delegate.getObject(index(columnIndex), type);
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        return delegate.getObject(name(columnLabel), type);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x, final SQLType targetSqlType, final int scaleOrLength) throws SQLException {
        delegate.updateObject(index(columnIndex), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x, final SQLType targetSqlType, final int scaleOrLength) throws SQLException {
        delegate.updateObject(name(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x, final SQLType targetSqlType) throws SQLException {
        delegate.updateObject(index(columnIndex), x, targetSqlType);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x, final SQLType targetSqlType) throws SQLException {
        delegate.updateObject(name(columnLabel), x, targetSqlType);
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }
}
