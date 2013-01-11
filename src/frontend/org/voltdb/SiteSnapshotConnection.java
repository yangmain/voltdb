/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.Deque;
import java.util.HashSet;
import java.util.Map;

import org.voltcore.utils.Pair;

/**
 * Defines the interface between a site and the snapshot
 * top-half.
 */
public interface SiteSnapshotConnection
{

    public void initiateSnapshots(
            Deque<SnapshotTableTask> tasks,
            long txnId,
            int numLiveHosts,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers);
    public HashSet<Exception> completeSnapshotWork() throws InterruptedException;

}
