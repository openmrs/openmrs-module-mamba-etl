package org.openmrs.module.ohrimamba.aop;

import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.Obs;
import org.openmrs.Patient;
import org.openmrs.api.context.Context;
import org.openmrs.module.ohrimamba.api.OHRIComputedConcept;
import org.openmrs.module.ohrimamba.engine.ConceptComputeTrigger;
import org.openmrs.module.ohrimamba.engine.OHRIComputedConceptsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.AfterReturningAdvice;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.openmrs.module.ohrimamba.engine.CommonsUUID.CAUSE_OF_DEATH;
import static org.openmrs.module.ohrimamba.engine.CommonsUUID.DATE_OF_DEATH;
import static org.openmrs.module.ohrimamba.engine.CommonsUUID.DEATH_FORM_ENCOUNTER_TYPE;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod date: 16/06/2021
 */
public class EncounterInterceptorAdvice implements AfterReturningAdvice {
	
	private static final Logger log = LoggerFactory.getLogger(EncounterInterceptorAdvice.class);
	
	@Override
    public void afterReturning(Object returnValue, Method methodInvoked, Object[] methodArgs, Object target)
            throws Throwable {

        try {
            if (methodInvoked.getName().equals(ConceptComputeTrigger.SAVE_ENCOUNTER)) {
                for (Object arg : methodArgs) {
                    if (arg instanceof Encounter) {
                        Encounter encounter = (Encounter) arg;
                        List<OHRIComputedConcept> ohriComputedConcepts = OHRIComputedConceptsFactory
                                .getComputedConcepts(encounter);
                        for (OHRIComputedConcept computedConcept : ohriComputedConcepts) {
                            computedConcept.computeAndPersistObs(encounter);
                        }

                        //TODO: Re-factor this code into a separate neat file
                        if (Objects.equals(encounter.getEncounterType().getUuid(), DEATH_FORM_ENCOUNTER_TYPE)) {

                            Supplier<Stream<Obs>> deathObs = encounter.getObs()::stream;
                            Date date = deathObs.get()
                                    .filter(obs -> obs.getConcept().getUuid().equals(DATE_OF_DEATH))
                                    .findAny()
                                    .map(Obs::getValueDate)
                                    .orElse(null);

                            if (date != null) {

                                Supplier<Stream<Obs>> causeObs = encounter.getObs()::stream;
                                Concept cause = causeObs.get()
                                        .filter(obs -> obs.getConcept().getUuid().equals(CAUSE_OF_DEATH))
                                        .findAny()
                                        .map(Obs::getValueCoded)
                                        .orElse(null);

                                Patient client = encounter.getPatient();
                                client.setCauseOfDeath(cause);
                                client.setDead(Boolean.TRUE);
                                client.setDeathDate(date);
                                Context.getPatientService().savePatient(client);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("An un-expected Error occurred while computing for a computed concept");
        }
    }
}
