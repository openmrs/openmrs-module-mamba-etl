package org.openmrs.module.ohrimamba.api.impl;

import org.openmrs.ohrimamba.ScriptRunner;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Main {
	
	public static void main(String[] args) {



        ScriptRunner runner = new ScriptRunner();
        System.out.println("Res dir herett: " + runner.getClass().getResource("/").getPath());

        URL resourceUrl = runner.getClass().getResource("/");
        System.out.println("Res dir herett 2: " + resourceUrl.getPath());

        try {
            runner.compileForMysql();
        } catch (TimeoutException | InterruptedException | ExecutionException | IOException e) {
            System.err.println("Error!");
            e.printStackTrace();
        }
    }
}
