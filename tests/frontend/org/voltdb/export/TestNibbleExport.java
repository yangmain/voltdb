/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.export;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltdb.BackendTarget;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.utils.VoltFile;

public class TestNibbleExport extends TestExportBaseSocketExport {

    public TestNibbleExport(String s) {
        super(s);
    }

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, 1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeClientAndServer();
    }

    static public junit.framework.Test suite() throws Exception {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(3));

        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestNibbleExport.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addLiteralSchema("CREATE TABLE nibble_export_table"
                + " (id integer not null,ts TIMESTAMP not null, PRIMARY KEY(id)) USING TTL 10 SECONDS ON COLUMN TS STREAM nibble_export_stream;\n"
                + " PARTITION TABLE nibble_export_table ON COLUMN id;\n"
                + " CREATE INDEX ttlindex ON nibble_export_table (ts);\n"
                + " CREATE STREAM nibble_export_stream PARTITION ON COLUMN id EXPORT TO TARGET "
                + " nibble_export_stream (id integer not null, ts TIMESTAMP not null);"
                );

        wireupExportTableToSocketExport("nibble_export_stream");

        LocalCluster cluster = new LocalCluster("testNibbleExport.jar", 8, 1, 0,
                BackendTarget.NATIVE_EE_JNI,  LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        cluster.setHasLocalServer(false);
        cluster.setNewCli(true);
        cluster.setMaxHeap(1024);

        boolean compile = cluster.compile(project);
        assertTrue(compile);
        builder.addServerConfig(cluster);
        return builder;
    }

    public void testNibbleExport() throws Exception {
        final Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        final int insertCount = 1000;
        for (int i=0; i < insertCount; i++) {
            client.callProcedure("@AdHoc", "INSERT INTO nibble_export_table VALUES(" + i + ",CURRENT_TIMESTAMP())");
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        waitForStreamedTargetAllocatedMemoryZero(client);
        VoltTable stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
        System.out.println(stats.toFormattedString());
        stats.resetRowPosition();
        int exportedCount = 0;
        while (stats.advanceRow()) {
            exportedCount += stats.getLong("TUPLE_COUNT");
        }
        assertTrue(insertCount <= exportedCount);
    }
}
