package org.openmrs.module.ohrimamba.api.impl;

import org.openmrs.ohrimamba.ScriptRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Main {
	
	public static void main(String[] args) {

        String compileScriptDirName = "src/main/resources/_core/database/mysql";
        ScriptRunner runner = new ScriptRunner();

        try {
            runner.execute();
        } catch (TimeoutException | InterruptedException | ExecutionException | IOException e) {
            System.err.println("Error!");
            e.printStackTrace();
        }
    }
}
