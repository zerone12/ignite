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

package org.apache.ignite;

import java.nio.charset.Charset;
import org.apache.ignite.internal.processors.query.h2.database.util.CompareUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ValueCompareTest extends GridCommonAbstractTest {
    /** */
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * @throws Exception If failed.
     */
    public void testUtfCompare() throws Exception {
        checkCompare("a", "b");

        checkCompare("aa", "a");

        checkCompare("в", "г");

        checkCompare("стр1", "str");

        checkCompare("стр", "str1");

        checkCompare("汉语", "漢語");

        checkCompare("中文", "汉语漢語");

        checkCompare("汉语", "string");

        checkCompare("汉语", "строка");
    }

    /**
     * @param str1 First string.
     * @param str2 Second string.
     * @throws Exception If failed.
     */
    private void checkCompare(String str1, String str2) throws Exception {
        copyAndCompare(str1, str1);

        copyAndCompare(str2, str2);

        copyAndCompare(str1, str2);

        copyAndCompare(str2, str1);
    }

    /**
     * @param str1 First string.
     * @param str2 Second string.
     * @throws Exception If failed.
     */
    private void copyAndCompare(String str1, String str2) throws Exception {
        int expCmp = normalizeCompareResult(str1.compareTo(str2));

        byte[] utf8Bytes = str1.getBytes(UTF_8);

        long addr = GridUnsafe.allocateMemory(utf8Bytes.length);

        try {
            GridUnsafe.copyMemory(utf8Bytes, GridUnsafe.BYTE_ARR_OFF, null, addr, utf8Bytes.length);

            int cmp = normalizeCompareResult(CompareUtils.compareUtf8(addr,
                utf8Bytes.length,
                str2));

            assertEquals(expCmp, cmp);

            byte[] utf8Bytes2 = str2.getBytes(UTF_8);

            cmp = normalizeCompareResult(CompareUtils.compareUtf8(addr,
                utf8Bytes.length,
                utf8Bytes2,
                0,
                utf8Bytes2.length));

            assertEquals(expCmp, cmp);
        }
        finally {
            GridUnsafe.freeMemory(addr);
        }
    }

    /**
     * @param cmp Compare result.
     * @return Normalized result (1, -1 or 0).
     */
    private int normalizeCompareResult(int cmp) {
        if (cmp < 0)
            return -1;
        else if (cmp > 0)
            return 1;

        return cmp;
    }
}
