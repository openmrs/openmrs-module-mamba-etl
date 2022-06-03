package org.openmrs.module.ohrimamba.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.Obs;
import org.openmrs.api.ConceptService;
import org.openmrs.api.EncounterService;
import org.openmrs.api.ObsService;
import org.openmrs.module.ohrimamba.aop.EncounterInterceptorAdvice;
import org.openmrs.module.ohrimamba.api.impl.HIVStatusComputedConcept;
import org.openmrs.module.ohrimamba.engine.CommonsUUID;
import org.openmrs.module.ohrimamba.engine.ConceptComputeTrigger;
import org.openmrs.module.ohrimamba.engine.HIVStatusConceptUUID;
import org.openmrs.test.BaseModuleContextSensitiveTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Method;
import java.util.List;

/**
 * @author MayanjaXL, Amos, Stephen, smallGod date: 07/07/2021
 */
public class HIVStatusComputedConceptTest extends BaseModuleContextSensitiveTest {
	
	protected static final String OHRI_INIT_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-StandardDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ATLEAST_ONE_POSITIVE_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-AtleastOnePositiveObsDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ATLEAST_ONE_NEGATIVE_BEYOND_90DAYS_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-OneNegativeObsBeyond90DaysDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ATLEAST_ONE_NEGATIVE_WITHIN_90DAYS_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-OneNegativeObsWithin90DaysDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ONE_NEGATIVE_WITHIN_90DAYS_PLUS_POSITIVE_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-OneNegativeObsWithin90DaysPlusPositiveDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_NO_FINAL_HIV_TEST_RESULT_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-NoFinalHIVTestObsDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ALL_OBS_VOIDED_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-AllVoidedObsDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ONE_POSITIVE_AND_ONE_VOIDED_OBS_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-OnePositiveOneVoidedObsDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ONE_NEGATIVE_IN_90DAYS_AND_ONE_POSITIVE_VOIDED_OBS_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-OneNegativeIn90DaysOnePositiveVoidedObsDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_ONE_NEGATIVE_IN_90DAYS_AND_ONE_UNKNOWN_OBS_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-OneNegativeIn90DaysOneUnknownObsDataset.xml";
	
	protected static final String OHRI_HIV_STATUS_EXISTING_POSITIVE_STATUS_XML_TEST_DATASET_PATH = "org/openmrs/module/ohrimamba/include/OHRI-ExistingComputedPositiveStatusObsDataset.xml";
	
	@Autowired
	private HIVStatusComputedConcept computedConcept;
	
	@Autowired
	private EncounterService encounterService;
	
	@Autowired
	private ConceptService conceptService;
	
	@Autowired
	private ObsService obsService;
	
	private Method methodInvoked;
	
	@Before
	public void initialiseCommonTestData() throws ClassNotFoundException, NoSuchMethodException {
		
		executeDataSet(OHRI_INIT_XML_TEST_DATASET_PATH);
		
		Class encounterServiceImplClass = Class.forName("org.openmrs.api.impl.EncounterServiceImpl");
		methodInvoked = encounterServiceImplClass.getDeclaredMethod(ConceptComputeTrigger.SAVE_ENCOUNTER, Encounter.class);
	}
	
	@Test
	public void compute_withOnePositiveObsShouldSetPositive() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ATLEAST_ONE_POSITIVE_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.POSITIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withOneNegativeObsBeyond90DaysShouldSetUnknown() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ATLEAST_ONE_NEGATIVE_BEYOND_90DAYS_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.UNKNOWN), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withOneNegativeObsIn90DaysNoPositiveShouldSetNegative() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ATLEAST_ONE_NEGATIVE_WITHIN_90DAYS_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.NEGATIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withOneNegativeObsIn90DaysPlusPositiveShouldSetPositive() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ONE_NEGATIVE_WITHIN_90DAYS_PLUS_POSITIVE_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.POSITIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withOneNegativeObsIn90DaysPlusMultipleNegativeBeyond90DaysShouldSetNegative() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ATLEAST_ONE_NEGATIVE_WITHIN_90DAYS_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.NEGATIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withNoFinalTestHIVObsShouldSetUnknown() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_NO_FINAL_HIV_TEST_RESULT_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.UNKNOWN), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withAllVoidedObsShouldSetUnknown() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ALL_OBS_VOIDED_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.UNKNOWN), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withOnePositiveAndOneVoidedObsShouldSetPositive() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ONE_POSITIVE_AND_ONE_VOIDED_OBS_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.POSITIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withOneNegativeObsIn90DaysAndOneVoidedShouldSetNegative() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ONE_NEGATIVE_IN_90DAYS_AND_ONE_POSITIVE_VOIDED_OBS_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.NEGATIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withOneNegativeObsIn90DaysAndOneUnknownSetNegative() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_ONE_NEGATIVE_IN_90DAYS_AND_ONE_UNKNOWN_OBS_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.NEGATIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withNegativeIn90DaysAndExistingComputedPositiveShouldSetPositive() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_EXISTING_POSITIVE_STATUS_XML_TEST_DATASET_PATH);
		executeDataSet(OHRI_HIV_STATUS_ATLEAST_ONE_NEGATIVE_WITHIN_90DAYS_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(computedConcept.getConcept(CommonsUUID.POSITIVE), hivTestObs.get(0).getValueCoded());
	}
	
	@Test
	public void compute_withNewPositiveAndExistingComputedPositiveShouldNotRecompute() throws Throwable {
		
		executeDataSet(OHRI_HIV_STATUS_EXISTING_POSITIVE_STATUS_XML_TEST_DATASET_PATH);
		executeDataSet(OHRI_HIV_STATUS_ATLEAST_ONE_POSITIVE_XML_TEST_DATASET_PATH);
		
		List<Obs> hivTestObs = computeComputedConceptHelper();
		
		Assert.assertEquals(180, hivTestObs.get(0).getId().intValue());//180 - obs id for the existing computed +ve
	}
	
	private List<Obs> computeComputedConceptHelper() throws Throwable {
		
		Encounter encounter = encounterService.getEncounter(100);//Test encounter ID
		new EncounterInterceptorAdvice().afterReturning(null, methodInvoked, new Object[] { encounter }, null);
		
		Concept hivStatus = conceptService.getConceptByUuid(HIVStatusConceptUUID.HIV_STATUS);
		List<Obs> hivTestObs = obsService.getObservationsByPersonAndConcept(encounter.getPatient().getPerson(), hivStatus);
		
		Assert.assertEquals("Expected only one computed HIV Status obs for this patient", 1, hivTestObs.size());
		
		return hivTestObs;
	}
}
