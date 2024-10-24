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
package org.jkiss.dbeaver.ext.cloudberry.edit;

import org.jkiss.dbeaver.ext.cloudberry.model.CloudberryDataSource;
import org.jkiss.dbeaver.ext.cloudberry.model.CloudberryExternalTable;
import org.jkiss.dbeaver.ext.cloudberry.model.CloudberrySchema;
import org.jkiss.dbeaver.ext.cloudberry.model.PostgreServerCloudberry;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreDatabase;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreDialect;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class CloudberryExternalTableManager2Test {
    @Mock
    private CloudberrySchema mockSchema;

    @Mock
    private PostgreDatabase mockDatabase;

    @Mock
    private ResultSet mockResults;

    @Mock
    private CloudberryDataSource mockDataSource;

    private CloudberryExternalTableManager cloudberryExternalTableManager;

    @Mock
    private PostgreServerCloudberry mockServerCloudberry;

    @Before
    public void setUp() {
        Mockito.when(mockDataSource.getSQLDialect()).thenReturn(new PostgreDialect());
        Mockito.when(mockSchema.getDatabase()).thenReturn(mockDatabase);
        Mockito.when(mockSchema.getDataSource()).thenReturn(mockDataSource);
        Mockito.when(mockDataSource.isServerVersionAtLeast(ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(false);
        Mockito.when(mockDataSource.getServerType()).thenReturn(mockServerCloudberry);

        cloudberryExternalTableManager = new CloudberryExternalTableManager();
    }

    @Test
    public void createDeleteAction_whenObjectIsAnExternalTable_thenExternalTableDropActionIsReturned() throws SQLException {
        SQLDatabasePersistAction regularTableDropTableQuery =
                new SQLDatabasePersistAction("Drop table", "DROP EXTERNAL TABLE foo.bar");

        Mockito.when(mockSchema.getName()).thenReturn("foo");
        Mockito.when(mockResults.getString("relname")).thenReturn("bar");

        CloudberryExternalTable cloudberryExternalTable = newCloudberryExternalTableFixture();

        SQLDatabasePersistAction sqlDatabasePersistAction =
                cloudberryExternalTableManager.createDeleteAction(cloudberryExternalTable, Collections.emptyMap());

        Assert.assertEquals(regularTableDropTableQuery.getScript(), sqlDatabasePersistAction.getScript());
    }

    @Test
    public void createDeleteAction_whenCascadeOptionIsProvided_thenExternalTableDropActionIsReturnedWithCascadeOption()
            throws SQLException {
        SQLDatabasePersistAction regularTableDropTableQuery =
                new SQLDatabasePersistAction("Drop table", "DROP EXTERNAL TABLE foo.bar CASCADE");

        Mockito.when(mockSchema.getName()).thenReturn("foo");
        Mockito.when(mockResults.getString("relname")).thenReturn("bar");

        CloudberryExternalTable cloudberryExternalTable = newCloudberryExternalTableFixture();

        SQLDatabasePersistAction sqlDatabasePersistAction =
                cloudberryExternalTableManager.createDeleteAction(cloudberryExternalTable,
                        Collections.singletonMap("deleteCascade", true));

        Assert.assertEquals(regularTableDropTableQuery.getScript(), sqlDatabasePersistAction.getScript());
    }

    private CloudberryExternalTable newCloudberryExternalTableFixture() throws SQLException {
        Mockito.when(mockResults.getString("fmttype")).thenReturn("b");
        Mockito.when(mockResults.getString("urilocation")).thenReturn("some_location");
        Mockito.when(mockResults.getString("fmtopts")).thenReturn("\n");
        Mockito.when(mockResults.getString("encoding")).thenReturn("UTF8");
        Mockito.when(mockResults.getString("execlocation")).thenReturn("some_location");
        return new CloudberryExternalTable(mockSchema, mockResults);
    }

}
