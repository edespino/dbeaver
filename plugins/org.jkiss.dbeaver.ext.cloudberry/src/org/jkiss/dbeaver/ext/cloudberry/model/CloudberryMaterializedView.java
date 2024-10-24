/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.cloudberry.model;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreMaterializedView;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreSchema;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreTableColumn;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.CommonUtils;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class CloudberryMaterializedView extends PostgreMaterializedView {

    private static final Log log = Log.getLog(CloudberryMaterializedView.class);

    private int[] distributionColumns;

    private boolean supportsReplicatedDistribution;

    public CloudberryMaterializedView(PostgreSchema catalog, ResultSet dbResult) {
        super(catalog, dbResult);

        if (catalog.getDataSource().isServerVersionAtLeast(9, 1)) {
            supportsReplicatedDistribution = true;
        }
    }

    public CloudberryMaterializedView(PostgreSchema catalog) {
        super(catalog);
    }

    private List<PostgreTableColumn> getDistributionPolicy(DBRProgressMonitor monitor) throws DBException {
        if (distributionColumns == null) {
            try {
                distributionColumns = CloudberryUtils.readDistributedColumns(monitor, this);
            } catch (Throwable e) {
                log.error("Error reading distribution policy", e);
            }
            if (distributionColumns == null) {
                distributionColumns = new int[0];
            }
        }

        if (distributionColumns.length == 0) {
            return null;
        }
        List<PostgreTableColumn> columns = new ArrayList<>(distributionColumns.length);
        for (int i = 0; i < distributionColumns.length; i++) {
            PostgreTableColumn attr = getAttributeByPos(monitor, distributionColumns[i]);
            if (attr == null) {
                log.debug("Bad policy attribute position: " + distributionColumns[i]);
            } else {
                columns.add(attr);
            }
        }
        return columns;
    }

    @Override
    public void appendTableModifiers(DBRProgressMonitor monitor, StringBuilder ddl) {
        try {
            List<PostgreTableColumn> distributionColumns = getDistributionPolicy(monitor);
            if (CommonUtils.isEmpty(distributionColumns)) {
                distributionColumns = CloudberryUtils.getDistributionTableColumns(monitor, distributionColumns, this);
            }

            CloudberryUtils.addObjectModifiersToDDL(monitor, ddl, this, distributionColumns, supportsReplicatedDistribution, false);
        } catch (DBException e) {
            log.error("Error reading Cloudberry table properties", e);
        }
    }
}
