package org.openmrs.module.ohrimamba.engine;

import org.openmrs.Encounter;
import org.openmrs.api.context.Context;
import org.openmrs.module.ohrimamba.api.OHRIComputedConcept;

import java.util.List;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod date: 28/06/2021
 */
public class OHRIComputedConceptsFactory {
	
	public static List<OHRIComputedConcept> getComputedConcepts(Encounter encounter) {
		//TODO:Use the encounter passed to narrow the list of ohriComputedConcepts based on an encounterType
		List<OHRIComputedConcept> ohriComputedConcepts = Context.getRegisteredComponents(OHRIComputedConcept.class);
		return ohriComputedConcepts;
	}
}
