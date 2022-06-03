package org.openmrs.module.ohrimamba.api.impl;

import org.openmrs.Obs;
import org.openmrs.module.ohrimamba.api.OHRIComputedConcept;
import org.springframework.stereotype.Component;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod date: 28/06/2021
 */
@Component
public class OnARTComputedConcept implements OHRIComputedConcept {
	
	@Override
	public org.openmrs.Obs compute(org.openmrs.Encounter triggeringEncounter) {
		return null;
	}
	
	@Override
	public org.openmrs.Concept getConcept() {
		return null;
	}
	
	@Override
	public Obs compareSavedComputedObs(Obs savedComputedObs, Obs newComputedObs) {
		return null;
	}
}
