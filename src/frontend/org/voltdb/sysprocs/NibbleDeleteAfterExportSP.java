/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SQLStmt;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

public class NibbleDeleteAfterExportSP  extends VoltSystemProcedure {

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        return null;
    }

    public VoltTable run(SystemProcedureExecutionContext ctx, int partitionParam,
            String tableName, String columnName, Object keys []) {

        Table catTable = ctx.getDatabase().getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException(
                    String.format("Table %s doesn't present in catalog", tableName));
        }
        Column column = catTable.getColumns().get(columnName);
        if (column == null) {
            throw new VoltAbortException(
                    String.format("Column %s does not exist in table %s", columnName, tableName));
        }

        ProcedureRunner pr = ctx.getSiteProcedureConnection().getNibbleExportDeleteProcRunner(
                tableName + ".autogenNibbleExportDelete", catTable, column);

        Procedure newCatProc = pr.getCatalogProcedure();
        Statement deleteStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "0");
        if (deleteStmt == null) {
            throw new VoltAbortException(String.format("Unable to find SQL statement for table %s.", tableName));
        }

        //TO DO: batch delete and aggregate results.
        VoltTable results = null;
        for (Object k : keys) {
            Object[] params = {k};
            results = executePrecompiledSQL(deleteStmt, params, false);
        }
        return results;
    }

    VoltTable executePrecompiledSQL(Statement catStmt, Object[] params, boolean replicated)
            throws VoltAbortException {
        SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
        if (replicated) {
            stmt.setInCatalog(false);
        }
        m_runner.initSQLStmt(stmt, catStmt);
        voltQueueSQL(stmt, params);
        return voltExecuteSQL()[0];
    }
}
