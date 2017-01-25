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

package org.apache.ignite.internal.processors.query.h2.database.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.query.h2.database.H2IntTree;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;

/**
 * Inner page for H2 row references.
 */
public class H2IntInnerIO extends BPlusInnerIO<SearchRow> {
    /** */
    public static final IOVersions<H2IntInnerIO> VERSIONS = new IOVersions<>(
        new H2IntInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private H2IntInnerIO(int ver) {
        super(T_H2_INT_REF_INNER, ver, true, 12);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(ByteBuffer buf, int off, SearchRow row) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, SearchRow row) {
        GridH2Row row0 = (GridH2Row)row;

        assert row0.link != 0;

        int val = row.getValue(row0.colId).getInt();
        assert val >= 0;

        PageUtils.putInt(pageAddr, off, val);
        PageUtils.putLong(pageAddr, off + 4, row0.link);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getLookupRow(BPlusTree<SearchRow,?> tree, long pageAddr, int idx, SearchRow r)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);

        assert link != 0;

        GridH2Row r0 = ((H2IntTree)tree).getRowFactory().getRow(link);

        if (r != null)
            r0.colId = ((GridH2Row)r).colId;

        return r0;
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<SearchRow> srcIo, long srcPageAddr, int srcIdx) {
        int srcOff = srcIo.offset(srcIdx);

        int val = PageUtils.getInt(srcPageAddr, srcOff);
        long link = PageUtils.getInt(srcPageAddr, srcOff + 4);

        assert link != 0;
        assert val >= 0;

        int dstOff = offset(dstIdx);

        PageUtils.putInt(dstPageAddr, dstOff, val);
        PageUtils.putLong(dstPageAddr, dstOff + 4, link);
    }

    private long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 4);
    }
}
