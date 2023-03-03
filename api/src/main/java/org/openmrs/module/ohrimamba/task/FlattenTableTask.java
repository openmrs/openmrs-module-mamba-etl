package org.openmrs.module.ohrimamba.task;

import org.openmrs.api.context.Context;
import org.openmrs.module.ohrimamba.api.FlattenDatabaseService;
import org.openmrs.scheduler.tasks.AbstractTask;

/**
 * @author Arthur D. Mugume
 */
public class FlattenTableTask extends AbstractTask {
	
	//@Autowired
	//FlattenDatabaseService flattenDatabaseService;
	
	@Override
	public void execute() {
		
		System.out.println("FlattenTableTask execute() called...");
		
		if (!isExecuting) {
			
			System.out.println("FlattenTableTask running...");
			startExecuting();
			
			try {
				getService().flattenDatabase();
			}
			catch (Exception e) {
				System.err.println("Error while running QueryLabResultsTask: " + e.getMessage());
				e.printStackTrace();
			}
			finally {
				stopExecuting();
				System.out.println("FlattenTableTask completed & stopped...");
			}
		} else {
			System.err.println("Error, Task already running, can't execute again");
		}
	}
	
	private FlattenDatabaseService getService() {
		return Context.getService(FlattenDatabaseService.class);
	}
}
