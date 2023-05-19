package org.openmrs.module.ohrimamba.api.dao.impl;

import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.procedure.ProcedureCall;
import org.hibernate.procedure.ProcedureOutputs;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.StringType;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.ohrimamba.api.dao.FlattenDatabaseDao;

import java.util.Date;
import java.util.List;

/**
 * @author smallGod date: 01/03/2023
 */
public class HibernateFlattenDatabaseDao implements FlattenDatabaseDao {
	
	private DbSessionFactory sessionFactory;
	
	@Override
	public void executeFlatteningScript() {
		
		//				SQLQuery query = sessionFactory.getCurrentSession().createSQLQuery("CALL sp_fact_tx_curr(?)");
		//				query.setParameter(1, "", StringType.INSTANCE);
		//				query.addScalar("output_parameter_name", StringType.INSTANCE);
		//				List<Object[]> rows = query.list();
		
		//				SQLQuery query2 = sessionFactory.getCurrentSession().createSQLQuery("CALL sp_fact_tx_curr(:end_date)");
		//				query2.setParameter("end_date", "end_date");
		//				query2.addScalar("drug_used", StandardBasicTypes.STRING);
		//				List<Object[]> rows2 = query2.list();
		
		//				SQLQuery query3 = sessionFactory.getCurrentSession().createSQLQuery("CALL sp_fact_tx_curr(:end_date, :age)");
		//				query3.setParameter("end_date", "end_date");
		//				query3.setParameter("age", "age");
		//				query3.addScalar("drug_used", StandardBasicTypes.STRING);
		//				query3.addScalar("reg_date", StandardBasicTypes.DATE);
		//				List<Object[]> rows3 = query3.list();
		//				for (Object[] result : rows3) {
		//					String drugUsed = (String) result[0];
		//					Date regDate = (Date) result[1];
		//				}
		
		//				SQLQuery query4 = sessionFactory.getCurrentSession().createSQLQuery("CALL sp_fact_tx_curr(?, ?)");
		//				query4.setParameter(0, "end_date");
		//				query4.setParameter(1, "age");
		//				query4.addScalar("drug_used", StandardBasicTypes.STRING);
		//				query4.addScalar("reg_date", StandardBasicTypes.DATE);
		//				List<Object[]> rows4 = query4.list();
		//				for (Object[] result : rows4) {
		//					String drugUsed = (String) result[0];
		//					Date regDate = (Date) result[1];
		//					// Do something with drugUsed and regDate
		//				}
		
		sessionFactory.getCurrentSession().createSQLQuery("CALL sp_data_processing_flatten()").executeUpdate();
		
		//		sessionFactory.getCurrentSession().doWork(connection -> {
		//		ProcedureCall call = sessionFactory
		//		        .getCurrentSession()
		//		        .createStoredProcedureCall("sp_data_processing");
		//        call.registerParameter()
		//		ProcedureOutputs outputs = call.getOutputs();
		//		});
		
	}
	
	public DbSessionFactory getSessionFactory() {
		return sessionFactory;
	}
	
	public void setSessionFactory(DbSessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
