package org.openmrs.module.ohrimamba.api;

import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.Location;
import org.openmrs.Obs;
import org.openmrs.Patient;
import org.openmrs.api.context.Context;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod
 * date: 28/06/2021
 */
public interface OHRIComputedConcept {

    public Obs compute(Encounter triggeringEncounter);

    public Concept getConcept();

    public default Concept getConcept(String UUID) {
        return Context.getConceptService().getConceptByUuid(UUID);
    }

    public default void persist(Obs obs) {
        Context.getObsService().saveObs(obs, "updated by Encounter interceptor");
    }

    public default void computeAndPersistObs(Encounter triggeringEncounter) {
        //TODO: throw an OHRI custom exception
        Obs obs = compute(triggeringEncounter);
        if (obs != null) {
            persist(obs);
        }
    }

    public default boolean keepHistory() {
        return false;
    }

    public default boolean isTimeBased() {
        return false;
    }

    default Obs initialiseAnObs(Patient patient, Concept targetConcept) {

        Obs computedObs = new Obs();
        computedObs.setDateCreated(new Date());
        computedObs.setObsDatetime(new Date());
        computedObs.setPerson(patient);
        computedObs.setConcept(getConcept());
        computedObs.setValueCoded(targetConcept);
        Location location = Context.getLocationService().getDefaultLocation();
        computedObs.setLocation(location);

        return computedObs;
    }

    default Obs getSavedComputedObs(Patient patient) {

        List<Obs> computedObsList = getObs(patient, getConcept());

        if (computedObsList == null || computedObsList.isEmpty()) {
            return null;
        }
        return Collections.max(computedObsList, Comparator.comparing(Obs::getDateCreated));
    }

    default List<Obs> getObs(Patient patient, Concept obsConcept) {

        return Context.getObsService()
                .getObservationsByPersonAndConcept(patient.getPerson(), obsConcept);
    }

    Obs compareSavedComputedObs(Obs savedComputedObs, Obs newComputedObs);
}
