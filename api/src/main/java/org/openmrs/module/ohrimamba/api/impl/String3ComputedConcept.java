package org.openmrs.module.ohrimamba.api.impl;

import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.Obs;
import org.openmrs.api.context.Context;
import org.openmrs.module.ohrimamba.api.OHRIComputedConcept;
import org.openmrs.module.ohrimamba.engine.String3ConceptUUID;
import org.springframework.stereotype.Component;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod date: 28/06/2021
 */

@Component("string3ComputedConcept")
public class String3ComputedConcept implements OHRIComputedConcept {
	
	@Override
	public Obs compute(Encounter triggeringEncounter) {
		
		String string1 = null;
		String string2 = null;
		for (Obs observation : triggeringEncounter.getObs()) {
			
			String value = observation.getValueText();
			if (value == null || value.trim().isEmpty()) {
				value = String.valueOf(observation.getValueNumeric());
			}
			
			String conceptUIID = observation.getConcept().getUuid();
			if (conceptUIID.equals(String3ConceptUUID.STRING1)) {
				string1 = value;
			} else if (conceptUIID.equals(String3ConceptUUID.STRING2)) {
				string2 = value;
			}
		}
		
		String string3Val = null;
		if (!(string1 == null || string2 == null)) {
			string3Val = string1 + " - " + string2;
		}
		//TODO: correct & uncomment
		//return createOrUpdate(triggeringEncounter.getPatient(), string3Val);//voids str3 if either str1 or str2 is Null
		return null;
	}
	
	@Override
	public Concept getConcept() {
		return Context.getConceptService().getConceptByUuid(String3ConceptUUID.STRING3);
	}
	
	@Override
	public Obs compareSavedComputedObs(Obs savedComputedObs, Obs newComputedObs) {
		return null;
	}
}
