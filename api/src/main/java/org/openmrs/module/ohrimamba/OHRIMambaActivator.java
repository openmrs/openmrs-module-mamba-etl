/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.ohrimamba;

import org.openmrs.api.context.Context;
import org.openmrs.module.BaseModuleActivator;
import org.openmrs.module.mambacore.api.FlattenDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Arthur D. Mugume, Laureen G. Omare
 * <p>
 * This class contains the logic that is run every time this module is either started or
 * shutdown
 */
public class OHRIMambaActivator extends BaseModuleActivator {

    private static final Logger log = LoggerFactory.getLogger(OHRIMambaActivator.class);

    public OHRIMambaActivator() {
        super();
    }

    /**
     * @see #started()
     */
    @Override
    public void started() {
        System.out.println("Started MambaETL Reference Module");
        log.info("Started MambaETL Reference Module");

        Context.getService(FlattenDatabaseService.class).flattenDatabase();
    }

    @Override
    public void stopped() {
        log.info("Stopped MambaETL Reference Module");
    }

    /**
     * @see #shutdown()
     */
    public void shutdown() {
        log.info("Shutdown MambaETL Reference Module");
    }

    public void willRefreshContext() {
        log.info("willRefreshContext MambaETL Reference Module");
    }

    @Override
    public void contextRefreshed() {
        log.info("log OHRIMambaActivator contextRefreshed()");
    }
}
