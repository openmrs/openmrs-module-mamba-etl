/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.ohrimamba;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.api.context.Context;
import org.openmrs.module.BaseModuleActivator;
import org.openmrs.module.ohrimambacore.task.FlattenTableTask;
import org.openmrs.scheduler.SchedulerException;
import org.openmrs.scheduler.Task;
import org.openmrs.scheduler.TaskDefinition;

import java.util.Calendar;
import java.util.UUID;

/**
 * @author Arthur D. Mugume, Laureen G. Omare
 *         <p>
 *         This class contains the logic that is run every time this module is either started or
 *         shutdown
 */
public class OHRIMambaActivator extends BaseModuleActivator {
	
	private static Log log = LogFactory.getLog(OHRIMambaActivator.class);
	
	/**
	 * @see #started()
	 */
	public void started() {
		
		log.info("MambaETL flattening Task...");
		System.out.println("MambaETL flattening Task...");
		registerTask("MambaETL Task", "MambaETL - Task to Flatten and Prepare Reporting Data.", FlattenTableTask.class,
		    60 * 60 * 12L, false);
	}
	
	/**
	 * @see #shutdown()
	 */
	public void shutdown() {
		log.info("Shutdown MambaETL Reference Module");
	}
	
	/**
	 * Register a new OpenMRS task
	 * 
	 * @param name the name
	 * @param description the description
	 * @param clazz the task class
	 * @param interval the interval in seconds
	 * @return boolean true if successful, else false
	 * @throws SchedulerException if task could not be scheduled
	 */
	private static boolean registerTask(String name, String description, Class<? extends Task> clazz, long interval,
	        boolean startOnStartup) {
		try {
			Context.addProxyPrivilege("Manage Scheduler");
			
			TaskDefinition taskDef = Context.getSchedulerService().getTaskByName(name);
			if (taskDef == null) {
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.MINUTE, 20);
				taskDef = new TaskDefinition();
				taskDef.setTaskClass(clazz.getCanonicalName());
				taskDef.setStartOnStartup(startOnStartup);
				taskDef.setRepeatInterval(interval);
				taskDef.setStarted(true);
				taskDef.setStartTime(cal.getTime());
				taskDef.setName(name);
				taskDef.setUuid(UUID.randomUUID().toString());
				taskDef.setDescription(description);
				Context.getSchedulerService().scheduleTask(taskDef);
				//Context.getSchedulerService().saveTaskDefinition(taskDef);
			}
			
		}
		catch (SchedulerException ex) {
			log.warn("Unable to register task '" + name + "' with scheduler", ex);
			return false;
		}
		finally {
			Context.removeProxyPrivilege("Manage Scheduler");
		}
		return true;
	}
}
