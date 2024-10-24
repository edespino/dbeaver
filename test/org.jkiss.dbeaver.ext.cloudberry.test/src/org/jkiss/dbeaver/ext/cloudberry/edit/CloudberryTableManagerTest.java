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
import org.jkiss.dbeaver.ext.cloudberry.model.CloudberrySchema;
import org.jkiss.dbeaver.ext.cloudberry.model.CloudberryTable;
import org.jkiss.dbeaver.ext.cloudberry.model.PostgreServerCloudberry;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreDatabase;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreDialect;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreTableForeign;
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
public class CloudberryTableManagerTest {
    @Mock
    private CloudberrySchema mockSchema;

    @Mock
    private PostgreDatabase mockDatabase;

    @Mock
    private ResultSet mockResults;

    @Mock
    private CloudberryDataSource mockDataSource;

    private CloudberryTableManager cloudberryTableManager;

    @Mock
    private PostgreServerCloudberry mockServerCloudberry;

    @Before
    public void setUp() {
        Mockito.when(mockDataSource.getSQLDialect()).thenReturn(new PostgreDialect());
        Mockito.when(mockSchema.getDatabase()).thenReturn(mockDatabase);
        Mockito.when(mockSchema.getDataSource()).thenReturn(mockDataSource);
        Mockito.when(mockDataSource.isServerVersionAtLeast(ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(false);
        Mockito.when(mockDataSource.getServerType()).thenReturn(mockServerCloudberry);

        cloudberryTableManager = new CloudberryTableManager();
    }

    @Test
    public void addObjectDeleteActions_whenObjectIsARegularTable_thenRegularTableDropActionIsReturned() throws SQLException {
        SQLDatabasePersistAction regularTableDropTableQuery =
                new SQLDatabasePersistAction("Drop table", "DROP TABLE foo.bar");

        Mockito.when(mockSchema.getName()).thenReturn("foo");
        Mockito.when(mockResults.getString("relname")).thenReturn("bar");
        CloudberryTable cloudberryTable = new CloudberryTable(mockSchema, mockResults);

        SQLDatabasePersistAction sqlDatabasePersistAction =
                cloudberryTableManager.createDeleteAction(cloudberryTable, Collections.emptyMap());

        Assert.assertEquals(regularTableDropTableQuery.getScript(), sqlDatabasePersistAction.getScript());
    }

    @Test
    public void addObjectDeleteActions_whenObjectIsAForeignTable_thenForeignTableDropActionIsReturned() throws SQLException {
        SQLDatabasePersistAction regularTableDropTableQuery =
                new SQLDatabasePersistAction("Drop table", "DROP FOREIGN TABLE foo.bar");

        Mockito.when(mockSchema.getName()).thenReturn("foo");
        Mockito.when(mockResults.getString("relname")).thenReturn("bar");

        PostgreTableForeign postgreForeignTable = new PostgreTableForeign(mockSchema, mockResults);

        SQLDatabasePersistAction sqlDatabasePersistAction =
                cloudberryTableManager.createDeleteAction(postgreForeignTable, Collections.emptyMap());

        Assert.assertEquals(regularTableDropTableQuery.getScript(), sqlDatabasePersistAction.getScript());
    }

    @Test
    public void addObjectDeleteActions_whenObjectIsATableWithCascadeOption_thenTableDropActionWithCascadeOptionIsReturned() throws SQLException {
        SQLDatabasePersistAction regularTableDropTableQuery =
                new SQLDatabasePersistAction("Drop table", "DROP TABLE foo.bar CASCADE");

        Mockito.when(mockSchema.getName()).thenReturn("foo");
        Mockito.when(mockResults.getString("relname")).thenReturn("bar");

        CloudberryTable cloudberryTable = new CloudberryTable(mockSchema, mockResults);

        SQLDatabasePersistAction sqlDatabasePersistAction =
                cloudberryTableManager.createDeleteAction(cloudberryTable,
                        Collections.singletonMap("deleteCascade", true));

        Assert.assertEquals(regularTableDropTableQuery.getScript(), sqlDatabasePersistAction.getScript());
    }
}