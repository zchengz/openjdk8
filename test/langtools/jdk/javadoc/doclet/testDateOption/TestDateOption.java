/*
 * Copyright (c) 2022, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * @test
 * @bug      8272984
 * @summary  javadoc support for SOURCE_DATE_EPOCH
 * @library  /tools/lib ../../lib
 * @modules  jdk.javadoc/jdk.javadoc.internal.tool
 * @build    toolbox.ToolBox javadoc.tester.*
 * @run main TestDateOption
 */

import java.io.BufferedReader;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javadoc.tester.JavadocTester;
import toolbox.ToolBox;

public class TestDateOption extends JavadocTester {

    /**
     * The entry point of the test.
     *
     * @param args the array of command line arguments
     * @throws Exception if the test fails
     */
    public static void main(String... args) throws Exception {
        TestDateOption tester = new TestDateOption();
        tester.runTests(m -> new Object[] { Path.of(m.getName()) });
    }

    ToolBox tb = new ToolBox();

    @Test
    public void testDateOption(Path base) throws Exception {
        Calendar c = Calendar.getInstance(); // uses current date, time, timezone etc
        // adjust the calendar to some date before the default used by javadoc (i.e. today)
        c.add(Calendar.DAY_OF_MONTH, -100);
        // set a specific time, such as 10 to 3. (Rupert Brooke, Grantchester)
        c.set(Calendar.HOUR, 2);
        c.set(Calendar.MINUTE, 50);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.AM_PM, Calendar.PM);
        Date testDate = c.getTime();
        out.println("Test Date: '" + testDate + "'");

        Path srcDir = base.resolve("src");
        tb.writeJavaFiles(srcDir, """
                package p;
                /** Comment. */
                public interface I { }
                """);
        Path outDir = base.resolve("out");

        javadoc("-d", outDir.toString(),
                "-sourcepath", srcDir.toString(),
                "--date", testDate.toInstant().toString(),
                "p");
        checkExit(Exit.OK);

        int featureVersion = Runtime.version().feature();
        String generatedByStamp = testDate.toString(); // matches what javadoc will use internally
        String generatedBy = String.format("<!-- Generated by javadoc (%d) on %s -->",
                featureVersion, generatedByStamp);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dcCreatedStamp = dateFormat.format(testDate);
        String dcCreated = String.format("""
                        <meta name="dc.created" content="%s">""",
                dcCreatedStamp);

        // check the timestamps in all generated HTML files
        for (Path file : tb.findFiles(".html", outputDir)) {
            checkOutput(outputDir.relativize(file).toString(), true,
                    generatedBy,
                    dcCreated);
        }
    }

    @Test
    public void testBadDateOption(Path base) throws Exception {
        Path srcDir = base.resolve("src");
        tb.writeJavaFiles(srcDir, """
                package p;
                /** Comment. */
                public interface I { }
                """);
        Path outDir = base.resolve("out");

        javadoc("-d", outDir.toString(),
                "-sourcepath", srcDir.toString(),
                "--date", "NOT A DATE",
                "p");
        checkExit(Exit.CMDERR);

        checkOutput(Output.OUT, true,
                "error: value for '--date' not valid: NOT A DATE");
    }

    @Test
    public void testInvalidDateOption(Path base) throws Exception {
        Path srcDir = base.resolve("src");
        tb.writeJavaFiles(srcDir, """
                package p;
                /** Comment. */
                public interface I { }
                """);
        Path outDir = base.resolve("out");

        javadoc("-d", outDir.toString(),
                "-sourcepath", srcDir.toString(),
                "--date", new Date(0).toInstant().toString(),
                "p");
        checkExit(Exit.CMDERR);

        checkOutput(Output.OUT, true,
                "error: value for '--date' out of range: 1970-01-01T00:00:00Z");
    }
}