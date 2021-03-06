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
 *
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for rebalancing.
 */
public class CacheRebalancingSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceFuture() throws Exception {
        IgniteEx ig0 = startGrid(0);

        startGrid(1);

        IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

        IgniteFuture fut1 = cache.rebalance();

        fut1.get();

        startGrid(2);

        IgniteFuture fut2 = cache.rebalance();

        assert internalFuture(fut2) != internalFuture(fut1);

        fut2.get();
    }

    /**
     * @param fut Future.
     * @return Internal future.
     */
    private static IgniteInternalFuture internalFuture(IgniteFuture fut) {
        assert fut instanceof IgniteFutureImpl : fut;

        return ((IgniteFutureImpl) fut).internalFuture();
    }
}
