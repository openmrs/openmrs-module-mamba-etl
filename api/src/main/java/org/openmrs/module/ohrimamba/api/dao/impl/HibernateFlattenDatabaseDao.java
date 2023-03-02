package org.openmrs.module.ohrimamba.api.dao.impl;

import org.hibernate.Query;
import org.hibernate.procedure.ProcedureCall;
import org.hibernate.procedure.ProcedureOutputs;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.ohrimamba.api.dao.FlattenDatabaseDao;

/**
 * @author smallGod date: 01/03/2023
 */
public class HibernateFlattenDatabaseDao implements FlattenDatabaseDao {
	
	private DbSessionFactory sessionFactory;
	
	@Override
	public void executeFlatteningScript() {
		
		Query query = sessionFactory.getCurrentSession().createSQLQuery("CALL sp_flat_encounter_table_insert_all()");
		query.executeUpdate();
		
		/*
		sessionFactory.getCurrentSession().doWork(connection -> {
		ProcedureCall call = sessionFactory
		        .getCurrentSession()
		        .createStoredProcedureCall("sp_flat_encounter_table_insert_all");
		ProcedureOutputs outputs = call.getOutputs();
		});
		 */
	}
	
	public DbSessionFactory getSessionFactory() {
		return sessionFactory;
	}
	
	public void setSessionFactory(DbSessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
