/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
 * Copyright (C) 2019 Dmitriy Dubson (ddubson@pivotal.io)
 * Copyright (C) 2019 Gavin Shaw (gshaw@pivotal.io)
 * Copyright (C) 2019 Zach Marcin (zmarcin@pivotal.io)
 * Copyright (C) 2019 Nikhil Pawar (npawar@pivotal.io)
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

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.postgresql.PostgreUtils;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreDataSource;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.osgi.framework.Version;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CloudberryDataSource extends PostgreDataSource {

    private static final Log log = Log.getLog(CloudberryDataSource.class);

    private Version gpVersion;
    private Boolean supportsFmterrtblColumn;
    private Boolean supportsRelstorageColumn;
    private Boolean hasAccessToExttable;

    public CloudberryDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container) throws DBException {
        super(monitor, container);
    }

    @Override
    public void initialize(@NotNull DBRProgressMonitor monitor) throws DBException {
        super.initialize(monitor);

        // Read server version
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Read Cloudberry server special info")) {
            String versionStr = JDBCUtils.queryString(session, "SELECT VERSION()");
            if (versionStr != null) {
                Matcher matcher = Pattern.compile("Cloudberry Database ([0-9\\.]+)").matcher(versionStr);
                if (matcher.find()) {
                    gpVersion = new Version(matcher.group(1));
                }
            }

            if (hasAccessToExttable == null) {
                hasAccessToExttable = PostgreUtils.isMetaObjectExists(session, "pg_exttable", "*");
            }
        } catch (Throwable e) {
            log.debug("Error reading GP server version", e);
        }
        if (gpVersion == null) {
            gpVersion = new Version(4, 2, 0);
        }
    }

    boolean isCloudberryVersionAtLeast(int major, int minor) {
        if (gpVersion == null) {
            log.debug("Can't read Cloudberry server version");
            return false;
        }
        if (gpVersion.getMajor() < major) {
            return false;
        } else return gpVersion.getMajor() != major || gpVersion.getMinor() >= minor;
    }

    boolean isHasAccessToExttable() {
        return hasAccessToExttable;
    }

    boolean isServerSupportFmterrtblColumn(@NotNull JDBCSession session) {
        if (supportsFmterrtblColumn == null) {
            if (!isHasAccessToExttable()) {
                supportsFmterrtblColumn = false;
            } else {
                supportsFmterrtblColumn = PostgreUtils.isMetaObjectExists(session, "pg_exttable", "fmterrtbl");
            }
        }
        return supportsFmterrtblColumn;
    }

    boolean isServerSupportsRelstorageColumn(@NotNull JDBCSession session) {
        if (supportsRelstorageColumn == null) {
            supportsRelstorageColumn = PostgreUtils.isMetaObjectExists(session, "pg_class", "relstorage");
        }
        return supportsRelstorageColumn;
    }

    @Association
    public boolean supportsExternalTables() {
        // External tables turned into foreign tables from version 7.
        // Let's check ability to use pg_exttable to show external tables correctly
        return isHasAccessToExttable();
    }
}
