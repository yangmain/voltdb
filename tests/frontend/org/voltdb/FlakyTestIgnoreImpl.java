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

package org.voltdb;

import org.voltdb.FlakyTestRule.FlakyTestIgnore;

public class FlakyTestIgnoreImpl implements FlakyTestIgnore {

    public boolean ignoreFlakyTest() {
        //System.out.println("DEBUG: FlakyTestIgnoreImpl.ignoreFlakyTest");

        // Get the value (if any) of the '-Drun.flaky.tests=...' system property,
        // specified on the command line
        String runFlakyPropVal = System.getProperty("run.flaky.tests", "DEFAULT");
        //System.out.println("DEBUG:     runFlakyPropVal: "+runFlakyPropVal);

        // When '-Drun.flaky.tests=FALSE' (or ='false', case insensitive),
        // don't run the tests - so do ignore them (return true)
        if ("FALSE".equalsIgnoreCase(runFlakyPropVal)) {
            return true;

        // By default, including when '-Drun.flaky.tests=TRUE' (or 'true', case
        // insensitive), do run the tests - so don't ignore them (return false)
        } else {
            // TODO! Should return false, but could not get ant to pass -D args,
            // so returning true, for now
            return true;
        }
    }

}
