package org.openmrs.module.ohrimamba.api.impl;

import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.Obs;
import org.openmrs.Patient;
import org.openmrs.api.context.Context;
import org.openmrs.module.ohrimamba.api.OHRIComputedConcept;
import org.openmrs.module.ohrimamba.engine.CommonsUUID;
import org.openmrs.module.ohrimamba.engine.HIVStatusConceptUUID;
import org.openmrs.module.ohrimamba.engine.ComputedConceptUtil;
import org.springframework.stereotype.Component;

import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod date: 28/06/2021
 */
@Component("hivStatusComputedConcept")
public class HIVStatusComputedConcept implements OHRIComputedConcept {
	
	@Override
	public Obs compute(Encounter triggeringEncounter) {
		
		Patient patient = triggeringEncounter.getPatient();
		
		Concept hivFinalTestConcept = getConcept(HIVStatusConceptUUID.FINAL_HIV_TEST_RESULT);
		List<Obs> hivTestObs = Context.getObsService().getObservationsByPersonAndConcept(patient.getPerson(),
		    hivFinalTestConcept);
		
		Concept newComputedConcept = computeHivStatusConcept(hivTestObs, getHIVFinalTestResultDate(patient));
		
		Obs newComputedObs = initialiseAnObs(patient, newComputedConcept);
		Obs savedComputedObs = getSavedComputedObs(patient);
		
		return compareSavedComputedObs(savedComputedObs, newComputedObs);
	}
	
	@Override
	public Obs compareSavedComputedObs(Obs savedComputedObs, Obs newComputedObs) {
		
		if (savedComputedObs == null) {
			return newComputedObs;
		}
		
		if (savedComputedObs.getValueCoded() == newComputedObs.getValueCoded()
		        || savedComputedObs.getValueCoded().equals(getConcept(CommonsUUID.POSITIVE))) {
			return null;
		}
		
		if (newComputedObs.getValueCoded().equals(getConcept(CommonsUUID.POSITIVE))
		        || savedComputedObs.getValueCoded().equals(getConcept(CommonsUUID.UNKNOWN))) {
			savedComputedObs.setValueCoded(newComputedObs.getValueCoded());
			return savedComputedObs;
		}
		
		return newComputedObs;
	}
	
	private Concept computeHivStatusConcept(List<Obs> hivTestObs, Date finalHivTestResultDate) {

        Supplier<Stream<Obs>> hivTestObsStream = hivTestObs::stream;
        return hivTestObsStream.get()
                .filter(obs -> obs.getValueCoded() == getConcept(CommonsUUID.POSITIVE))
                .findAny()
                .map(Obs::getValueCoded)
                .orElse(hivTestObsStream.get()
                        .filter(obs -> obs.getValueCoded() == getConcept(CommonsUUID.NEGATIVE))
                        .filter(obs -> ComputedConceptUtil.dateWithinPeriodFromNow(finalHivTestResultDate, ChronoUnit.DAYS, -90))
                        .findAny()
                        .map(Obs::getValueCoded)
                        .orElse(getConcept(CommonsUUID.UNKNOWN))
                );
    }
	
	public Date getHIVFinalTestResultDate(Patient patient) {

        Concept hivFinalTestDateConcept = getConcept(HIVStatusConceptUUID.HIV_TEST_RESULT_DATE);

        List<Obs> hivTestObs = Context.getObsService()
                .getObservationsByPersonAndConcept(patient.getPerson(), hivFinalTestDateConcept);

        Supplier<Stream<Obs>> hivTestObsStream = hivTestObs::stream;
        return hivTestObsStream.get()
                .findAny()//TODO: Might need to filter out the exact concept for this test date
                .map(Obs::getValueDate)
                .orElse(null);
    }
	
	public Optional<Obs> getPositiveComputedHivStatus(Patient patient) {

        List<Obs> computedHivObs = Context.getObsService()
                .getObservationsByPersonAndConcept(patient.getPerson(), getConcept());

        Supplier<Stream<Obs>> hivTestObsStream = computedHivObs::stream;

        return hivTestObsStream.get()
                .filter(obs -> obs.getValueCoded() == getConcept(CommonsUUID.POSITIVE))
                .findFirst();
    }
	
	@Override
	public Concept getConcept() {
		return Context.getConceptService().getConceptByUuid(HIVStatusConceptUUID.HIV_STATUS);
	}
}
