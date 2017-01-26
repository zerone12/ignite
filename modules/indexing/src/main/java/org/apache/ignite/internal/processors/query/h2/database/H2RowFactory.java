/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryPrimitives;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOffheapInputStream;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.database.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.util.MathUtils;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueUuid;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.processors.query.h2.database.util.CompareUtils.compareBoolean;
import static org.apache.ignite.internal.processors.query.h2.database.util.CompareUtils.compareByte;
import static org.apache.ignite.internal.processors.query.h2.database.util.CompareUtils.compareUtf8;
import static org.apache.ignite.internal.processors.query.h2.database.util.CompareUtils.convertToByte;

/**
 * Data store for H2 rows.
 */
public class H2RowFactory {
    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final GridH2RowDescriptor rowDesc;

    /** */
    private final CacheObjectContext coctx;


    /**
     * @param rowDesc Row descriptor.
     * @param cctx Cache context.
     */
    public H2RowFactory(GridH2RowDescriptor rowDesc, GridCacheContext<?,?> cctx) {
        this.rowDesc = rowDesc;
        this.cctx = cctx;
        coctx = cctx.cacheObjectContext();
    }

    /**
     * !!! This method must be invoked in read or write lock of referring index page. It is needed to
     * !!! make sure that row at this link will be invisible, when the link will be removed from
     * !!! from all the index pages, so that row can be safely erased from the data page.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Row getRow(long link) throws IgniteCheckedException {
        // TODO Avoid extra garbage generation. In upcoming H2 1.4.193 Row will become an interface,
        // TODO we need to refactor all this to return CacheDataRowAdapter implementing Row here.

        final CacheDataRowAdapter rowBuilder = new CacheDataRowAdapter(link);

        rowBuilder.initFromLink(cctx, false);

        GridH2Row row;

        try {
            row = rowDesc.createRow(rowBuilder.key(),
                PageIdUtils.partId(link), rowBuilder.value(), rowBuilder.version(), rowBuilder.expireTime());

            row.link = link;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        assert row.ver != null;

        return row;
    }

    /**
     * @param link Link.
     * @return Compare result.
     * @throws IgniteCheckedException If failed.
     */
    public int compareRows(long link, SearchRow row, H2TreeIndex idx0) throws IgniteCheckedException {
        PageMemory pageMem = cctx.shared().database().pageMemory();

        try (Page page = pageMem.page(cctx.cacheId(), PageIdUtils.pageId(link))) {
            long pageAddr = page.getForReadPointer(); // Non-empty data page must not be recycled.

            assert pageAddr != 0L : link;

            try {
                DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                DataPagePayload data = io.readPayload(pageAddr,
                    itemId(link),
                    pageMem.pageSize());

                if (data.nextLink() != 0)
                    throw new IgniteCheckedException("Multi page record");

                final long dataAddr = pageAddr + data.offset();

                IndexColumn[] idxCols = idx0.getIndexColumns();
                int[] colIds = idx0.columnIds();
                GridH2Table tbl = idx0.getTable();

                //assert idxVals.length == idxCols.length;

                // TODO: check key/value cache object type. Handle BIG_ENDIAN.

                final long keyStartAddr = dataAddr;
                int keyLen = PageUtils.getInt(keyStartAddr, 0);
                // byte keyType = GridUnsafe.unsafe().getByte(keyAddr + 4);

                GridH2RowDescriptor rowDesc = tbl.rowDescriptor();

                long cmpValAddr;
                int dataLen;

                GridH2Row h2Row;

                if (row instanceof GridH2Row && ((GridH2Row)row).key != null&& ((GridH2Row)row).val != null)
                    h2Row = (GridH2Row)row;
                else
                    h2Row = null;

                for (int i = 0, len = idxCols.length; i < len; i++) {
                    int idx = colIds[i];

                    byte[] valBytes = null;
                    int valOff = 0;

                    int type;

                    if (idx < GridH2AbstractKeyValueRow.DEFAULT_COLUMNS_COUNT) {
                        if (idx == GridH2AbstractKeyValueRow.KEY_COL) {
                            type = rowDesc.keyType();

                            cmpValAddr = keyStartAddr + 5;
                            dataLen = keyLen;

                            if (h2Row != null) {
                                valBytes = h2Row.key.valueBytes(coctx);
                                valOff = 0;
                            }
                        }
                        else {
                            assert idx == GridH2AbstractKeyValueRow.VAL_COL : idx;

                            long valAddr = keyStartAddr + keyLen + 5;
                            dataLen = PageUtils.getInt(valAddr, 0);
                            // byte valType = GridUnsafe.unsafe().getByte(valAddr + 4);

                            type = rowDesc.valueType();
                            cmpValAddr = valAddr + 5;

                            if (h2Row != null) {
                                valBytes = h2Row.val.valueBytes(coctx);
                                valOff = 0;
                            }
                        }
                    }
                    else {
                        idx -= GridH2AbstractKeyValueRow.DEFAULT_COLUMNS_COUNT;

                        assert idx >= 0 : idx;

                        type = rowDesc.fieldType(idx);

                        GridQueryProperty prop = rowDesc.property(idx);

                        long valAddr = keyStartAddr + keyLen + 5;
                        int valLen = PageUtils.getInt(valAddr, 0);

                        int off = prop.propertyOffset(keyStartAddr + 5, keyLen, valAddr + 5, valLen);

                        if (off == -1)
                            return 0;

                        if (prop.key()) {
                            cmpValAddr = keyStartAddr + 5 + off;
                            dataLen = keyLen - off;
                        }
                        else {
                            cmpValAddr = valAddr + 5 + off;
                            dataLen = valLen - off;
                        }

                        if (h2Row != null) {
                            valOff = prop.propertyOffset(h2Row.key, h2Row.val, coctx);

                            if (prop.key())
                                valBytes = h2Row.key.valueBytes(coctx);
                            else
                                valBytes = h2Row.val.valueBytes(coctx);
                        }
                    }

                    assert cmpValAddr > 0 : cmpValAddr;

                    int comp;

                    if (valBytes != null) {
                        if (valOff == -1)
                            return 0;

                        comp = compareValues(cmpValAddr, dataLen, type, valBytes, valOff, tbl);
                    }
                    else {
                        Value v2 = row.getValue(colIds[i]);

                        if (v2 == null)
                            return 0;

                        int dataType = Value.getHigherOrder(type, v2.getType());

                        v2 = v2.convertTo(dataType);

                        comp = compareValues(cmpValAddr, dataLen, type, dataType, v2, tbl);
                    }

                    if (comp != 0) {
                        int sortType = idxCols[i].sortType;

                        if ((sortType & SortOrder.DESCENDING) != 0)
                            comp = -comp;

                        return comp;
                    }
                }

                return 0;
            }
            finally {
                page.releaseRead();
            }
        }
    }

    /**
     * @param val1Addr Marshalled value address.
     * @param val1Len Data length.
     * @param h2Type Expected value type.
     * @param val2Bytes Second value marshalled.
     * @param val2Off Second value offset.
     * @return Compare result.
     * @throws IgniteCheckedException If failed.
     */
    private int compareValues(long val1Addr,
                              int val1Len,
                              int h2Type,
                              byte[] val2Bytes,
                              int val2Off,
                              GridH2Table tbl) throws IgniteCheckedException {
        byte type1 = PageUtils.getByte(val1Addr, 0);

        if (type1 == GridBinaryMarshaller.NULL)
            return 0;

        byte type2 = val2Bytes[val2Off];

        if (type2 == GridBinaryMarshaller.NULL)
            return 0;

        switch (h2Type) {
            case Value.INT: {
                checkType(GridBinaryMarshaller.INT, type1, type2);

                int v1 = BinaryPrimitives.readInt(val1Addr, 1);
                int v2 = BinaryPrimitives.readInt(val2Bytes, val2Off + 1);

                return MathUtils.compareInt(v1, v2);
            }

            case Value.LONG: {
                checkType(GridBinaryMarshaller.LONG, type1, type2);

                long v1 = BinaryPrimitives.readLong(val1Addr, 1);
                long v2 = BinaryPrimitives.readLong(val2Bytes, val2Off + 1);

                return MathUtils.compareLong(v1, v2);
            }

            case Value.STRING: {
                checkType(GridBinaryMarshaller.STRING, type1, type2);

                val1Addr++;

                int len1 = PageUtils.getInt(val1Addr, 0);

                int len2 = BinaryPrimitives.readInt(val2Bytes, val2Off + 1);

                return compareUtf8(val1Addr + 4, len1, val2Bytes, val2Off + 5, len2);
            }

            case Value.BYTE: {
                checkType(GridBinaryMarshaller.BYTE, type1, type2);

                byte v1 = BinaryPrimitives.readByte(val1Addr, 1);
                byte v2 = BinaryPrimitives.readByte(val2Bytes, val2Off + 1);

                return MathUtils.compareInt(v1, v2);
            }

            case Value.SHORT: {
                checkType(GridBinaryMarshaller.SHORT, type1, type2);

                short v1 = BinaryPrimitives.readShort(val1Addr, 1);
                short v2 = BinaryPrimitives.readShort(val2Bytes, val2Off + 1);

                return MathUtils.compareInt(v1, v2);
            }

            case Value.FLOAT: {
                checkType(GridBinaryMarshaller.FLOAT, type1, type2);

                float v1 = BinaryPrimitives.readFloat(val1Addr, 1);
                float v2 = BinaryPrimitives.readFloat(val2Bytes, val2Off + 1);

                return Float.compare(v1, v2);
            }

            case Value.DOUBLE: {
                checkType(GridBinaryMarshaller.DOUBLE, type1, type2);

                double v1 = BinaryPrimitives.readDouble(val1Addr, 1);
                double v2 = BinaryPrimitives.readDouble(val2Bytes, val2Off + 1);

                return Double.compare(v1, v2);
            }

            case Value.BOOLEAN: {
                checkType(GridBinaryMarshaller.BOOLEAN, type1, type2);

                boolean v1 = BinaryPrimitives.readBoolean(val1Addr, 1);
                boolean v2 = BinaryPrimitives.readBoolean(val2Bytes, val2Off + 1);

                return (v1 == v2) ? 0 : (v1 ? 1 : -1);
            }

            case Value.UUID: {
                checkType(GridBinaryMarshaller.UUID, type1, type2);

                long high1 = BinaryPrimitives.readLong(val1Addr, 1);
                long high2 = BinaryPrimitives.readLong(val2Bytes, val2Off + 1);

                if (high1 == high2) {
                    long low1 = BinaryPrimitives.readLong(val1Addr, 9);
                    long low2 = BinaryPrimitives.readLong(val2Bytes, val2Off + 9);

                    return MathUtils.compareLong(low1, low2);
                }
                else
                    return high1 > high2 ? 1 : -1;
            }

            case Value.DATE: {
                checkType(GridBinaryMarshaller.DATE, type1, type2);

                long time1 = BinaryPrimitives.readLong(val1Addr, 1);
                time1 = GridH2Utils.dateFromMillis(time1);

                long time2 = BinaryPrimitives.readLong(val2Bytes, val2Off);
                time2 = GridH2Utils.dateFromMillis(time2);

                return MathUtils.compareLong(time1, time2);
            }

            case Value.TIMESTAMP: {
                long ms1;
                long time1;

                if (type1 == GridBinaryMarshaller.DATE || type1 == GridBinaryMarshaller.TIMESTAMP) {
                    ms1 = BinaryPrimitives.readLong(val1Addr, 1);
                    time1 = GridH2Utils.dateFromMillis(ms1);
                }
                else
                    throw new IgniteCheckedException("Invalid type: " + type1);

                long ms2;
                long time2;

                if (type2 == GridBinaryMarshaller.DATE || type2 == GridBinaryMarshaller.TIMESTAMP) {
                    ms2 = BinaryPrimitives.readLong(val2Bytes, val2Off + 1);
                    time2 = GridH2Utils.dateFromMillis(ms2);
                }
                else
                    throw new IgniteCheckedException("Invalid type: " + type2);

                int cmp = MathUtils.compareLong(time1, time2);

                if (cmp != 0)
                    return cmp;

                long nanos1;

                if (type1 == GridBinaryMarshaller.DATE)
                    nanos1 = GridH2Utils.nanosFromMillis(ms1);
                else {
                    nanos1 = BinaryPrimitives.readInt(val1Addr, 9);

                    nanos1 += GridH2Utils.nanosFromMillis(ms1);
                }

                long nanos2;

                if (type2 == GridBinaryMarshaller.DATE)
                    nanos2 = GridH2Utils.nanosFromMillis(ms2);
                else {
                    nanos2 = BinaryPrimitives.readInt(val2Bytes, val2Off + 9);

                    nanos2 += GridH2Utils.nanosFromMillis(ms2);
                }

                return MathUtils.compareLong(nanos1, nanos2);
            }

            // TODO: avoid value unmarshal for all types where possible.
            default: {
                GridH2RowDescriptor rowDesc = tbl.rowDescriptor();

                Object obj1 = unmarshal(val1Addr, val1Len);
                Value val1 = rowDesc.wrap(obj1, h2Type);

                Object obj2 = unmarshal(val2Bytes, val2Off);
                Value val2 = rowDesc.wrap(obj2, h2Type);

                return tbl.compareTypeSafe(val1, val2);
            }
        }
    }

    /**
     * @param valAddr Marshalled value address.
     * @param len Data length.
     * @param h2Type Expected value type.
     * @param h2TargetType Type value should be converted to.
     * @param val Value to compare with.
     * @return Compare result.
     * @throws IgniteCheckedException If failed.
     */
    private int compareValues(long valAddr,
                              int len,
                              int h2Type,
                              int h2TargetType,
                              Value val,
                              GridH2Table tbl) throws IgniteCheckedException {
        byte type = PageUtils.getByte(valAddr, 0);

        if (type == GridBinaryMarshaller.NULL)
            return 0;

        switch (h2Type) {
            case Value.INT: {
                if (type != GridBinaryMarshaller.INT)
                    throw new IgniteCheckedException("Invalid type: " + type);

                int v = BinaryPrimitives.readInt(valAddr, 1);

                if (h2Type == h2TargetType)
                    return MathUtils.compareInt(v, val.getInt());

                switch (h2TargetType) {
                    case Value.BOOLEAN: {
                        boolean v0 = Integer.signum(v) != 0;

                        return compareBoolean(v0, val);
                    }

                    case Value.BYTE: {
                        byte v0 = convertToByte(v);

                        return compareByte(v0, val);
                    }

                    default:
                        throw new IgniteCheckedException("Data conversion is undefined.");
                }
            }

            case Value.LONG: {
                if (type != GridBinaryMarshaller.LONG)
                    throw new IgniteCheckedException("Invalid type: " + type);

                long v = BinaryPrimitives.readLong(valAddr, 1);

                if (h2Type == h2TargetType)
                    return MathUtils.compareLong(v, val.getLong());
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.STRING: {
                if (type != GridBinaryMarshaller.STRING)
                    throw new IgniteCheckedException("Invalid type: " + type);

                String str = val.getString();

                int dataLen = BinaryPrimitives.readInt(valAddr, 1);

                if (h2Type == h2TargetType)
                    return compareUtf8(valAddr + 5, dataLen, str);
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.BYTE: {
                if (type != GridBinaryMarshaller.BYTE)
                    throw new IgniteCheckedException("Invalid type: " + type);

                byte v = BinaryPrimitives.readByte(valAddr, 1);

                if (h2Type == h2TargetType)
                    return compareByte(v, val);
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.SHORT: {
                if (type != GridBinaryMarshaller.SHORT)
                    throw new IgniteCheckedException("Invalid type: " + type);

                short v = BinaryPrimitives.readShort(valAddr, 1);

                if (h2Type == h2TargetType)
                    return MathUtils.compareInt(v, val.getShort());
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.FLOAT: {
                if (type != GridBinaryMarshaller.FLOAT)
                    throw new IgniteCheckedException("Invalid type: " + type);

                float v = BinaryPrimitives.readFloat(valAddr, 1);

                if (h2Type == h2TargetType)
                    return Float.compare(v, val.getFloat());
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.DOUBLE: {
                if (type != GridBinaryMarshaller.DOUBLE)
                    throw new IgniteCheckedException("Invalid type: " + type);

                double v = BinaryPrimitives.readDouble(valAddr, 1);

                if (h2Type == h2TargetType)
                    return Double.compare(v, val.getDouble());
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.BOOLEAN: {
                if (type != GridBinaryMarshaller.BOOLEAN)
                    throw new IgniteCheckedException("Invalid type: " + type);

                boolean v = BinaryPrimitives.readBoolean(valAddr, 1);

                if (h2Type == h2TargetType)
                    return compareBoolean(v, val);
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.UUID: {
                if (type != GridBinaryMarshaller.UUID)
                    throw new IgniteCheckedException("Invalid type: " + type);

                ValueUuid valUuid = (ValueUuid)val;

                if (h2Type == h2TargetType) {
                    long high1 = BinaryPrimitives.readLong(valAddr, 1);
                    long high2 = valUuid.getHigh();

                    if (high1 == high2) {
                        long low = BinaryPrimitives.readLong(valAddr, 9);

                        return MathUtils.compareLong(low, valUuid.getLow());
                    }
                    else
                        return high1 > high2 ? 1 : -1;
                }
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            case Value.DATE: {
                if (type != GridBinaryMarshaller.DATE)
                    throw new IgniteCheckedException("Invalid type: " + type);

                if (h2Type == h2TargetType) {
                    long time = BinaryPrimitives.readLong(valAddr, 1);

                    time = GridH2Utils.dateFromMillis(time);

                    ValueDate valDate = (ValueDate)val;

                    return MathUtils.compareLong(time, valDate.getDateValue());
                }
                else
                    throw new UnsupportedOperationException("Type conversion not implemented.");
            }

            // TODO: avoid value unmarshal for all types where possible.
            default:
                GridH2RowDescriptor rowDesc = tbl.rowDescriptor();

                Object obj = unmarshal(valAddr, len);

                Value h2Val = rowDesc.wrap(obj, h2Type);

                h2Val = h2Val.convertTo(h2TargetType);

                return h2Val.compareTypeSafe(val, tbl.getCompareMode());
        }
    }

    /**
     * @param expType Expected type.
     * @param type1 Actual type.
     * @param type2 Actual type.
     * @throws IgniteCheckedException If check failed.
     */
    private static void checkType(int expType, int type1, int type2) throws IgniteCheckedException {
        if (expType != type1)
            throw new IgniteCheckedException("Invalid type: " + type1);

        if (expType != type2)
            throw new IgniteCheckedException("Invalid type: " + type2);
    }

    /**
     * @param valAddr Value address.
     * @param len Value length.
     * @return Unmarshalled object.
     */
    private Object unmarshal(long valAddr, int len) {
        CacheObjectBinaryProcessorImpl proc = (CacheObjectBinaryProcessorImpl)cctx.cacheObjects();

        BinaryOffheapInputStream in = new BinaryOffheapInputStream(valAddr, len);

        return BinaryUtils.unmarshal(in, proc.binaryContext(), null);
    }

    /**
     * @param arr Marshalled value.
     * @param off Marshalled value offset.
     * @return Unmarshalled object.
     */
    private Object unmarshal(byte[] arr, int off) {
        CacheObjectBinaryProcessorImpl proc = (CacheObjectBinaryProcessorImpl)cctx.cacheObjects();

        return BinaryUtils.unmarshal(BinaryHeapInputStream.create(arr, off), proc.binaryContext(), null);
    }

}
