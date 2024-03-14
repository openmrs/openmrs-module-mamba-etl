/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.mambaetl;

import org.databene.commons.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.Assert.*;

/**
 * date: 17/01/2024
 */
public class MambaTest {
	
	private static Connection connection;
	
	@BeforeClass
    public static void setUp() {

        try {
            String jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
            String username = "mamba_user";
            String password = "mamba_pass";

            connection = DriverManager.getConnection(jdbcUrl, username, password);

            try (Statement statement = connection.createStatement()) {
                int affectedRows = statement.executeUpdate("CREATE SCHEMA analysis_db");
                assertEquals("Unexpected number of affected rows.", 0, affectedRows);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to create db connection or set up the in-memory database.", e);
        }
    }
	
	@AfterClass
	public static void tearDown() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}
	
	@Test
    public void shouldConfirmAnalysisDatabaseExists() {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'analysis_db'")) {
                assertNotNull("ResultSet is null", resultSet);
            }
        } catch (SQLException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }
	
	@Test
	public void shouldReturnValidDatabaseConnection() {
		assertNotNull("Connection should not be null", connection);
	}
	
	private void executeStoredProcedure(String procedureName, String parameterValue) throws SQLException {
        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?)}")) {

            callableStatement.setString(1, parameterValue);
            callableStatement.execute();
        }
    }
	
	private static String readScriptFile(String filePath) {
        StringBuilder content = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading script file.", e);
        }
        return content.toString();
    }
	
	@Test
	public void shouldGenerateFolderUnderResourcesNamedMamba() {
		
		String folderName = "../api/src/main/resources/mamba";
		
		Path folderPath = Paths.get(folderName);
		
		Assert.isTrue(Files.isDirectory(folderPath), "Folder '" + folderName + "' not found under resources folder");
	}
	
	//@Test
	public void shouldGenerateFile_CreateStoredProcedures() {
		
		String fileName = "../api/src/main/resources/mamba/create_stored_procedures.sql";
		
		Path filePath = Paths.get(fileName);
		
		assertTrue("Build file '" + fileName + "' not created under resources folder", Files.exists(filePath));
	}
	
	//@Test
	public void shouldGenerateFile_LiquibaseCreateStoredProcedures() {
		
		String fileName = "../api/src/main/resources/mamba/liquibase_create_stored_procedures.sql";
		
		Path filePath = Paths.get(fileName);
		
		assertTrue("Build file '" + fileName + "' not created under resources folder", Files.exists(filePath));
	}
}
