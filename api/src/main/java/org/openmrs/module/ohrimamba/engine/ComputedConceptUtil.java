package org.openmrs.module.ohrimamba.engine;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod date: 21/07/2021
 */
public final class ComputedConceptUtil {
	
	/**
	 * Check if a given date falls within a given period/duration from Now
	 * 
	 * @param dateToCheck the date to check if it falls within a certain period/duration
	 * @param periodUnit Can be hours, days, years etc
	 * @param periodAmount Period amount is negative if in the past or Positive if in the future
	 * @return true if given date is within duration, otherwise false
	 */
	public static boolean dateWithinPeriodFromNow(Date dateToCheck, ChronoUnit periodUnit, int periodAmount) {
		
		if (dateToCheck == null) {
			throw new IllegalArgumentException("dateToCheck argument is null, expects a non-null value");
		}
		
		LocalDateTime targetDateTime = LocalDateTime.now().plus(periodAmount, periodUnit);
		LocalDateTime dateTimeToCheck = LocalDateTime.ofInstant(dateToCheck.toInstant(), ZoneId.systemDefault());
		return dateTimeToCheck.isAfter(targetDateTime);
	}
}
