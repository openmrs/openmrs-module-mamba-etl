package org.openmrs.module.ohrimamba.core;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Arthur M.D
 */
public class ScriptRunner {

    public void execute() {

        ProcessBuilder builder = new ProcessBuilder();

        builder.
        builder.directory(new File(System.getProperty("user.home")));
        Process process = builder.start();
        StreamGobbler streamGobbler =
                new StreamGobbler(process.getInputStream(), System.out::println);
        Future<?> future = Executors.newSingleThreadExecutor().submit(streamGobbler);
        int exitCode = process.waitFor();
        assert exitCode == 0;
        future.get(10, TimeUnit.SECONDS);
    }
}
