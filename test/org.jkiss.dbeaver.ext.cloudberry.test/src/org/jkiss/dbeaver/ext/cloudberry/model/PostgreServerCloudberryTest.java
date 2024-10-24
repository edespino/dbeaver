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
import org.jkiss.dbeaver.ext.postgresql.model.PostgreClass;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreDialect;
import org.jkiss.dbeaver.ext.postgresql.model.PostgreSchema;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;

@RunWith(MockitoJUnitRunner.class)
public class PostgreServerCloudberryTest {
    @Mock
    CloudberryDataSource mockDataSource;

    @Mock
    PostgreSchema mockSchema;

    @Mock
    JDBCResultSet mockResults;

    @Mock
    DBRProgressMonitor monitor;

    @InjectMocks
    PostgreServerCloudberry server;

    @Before
    public void setup() throws SQLException {
        Mockito.when(mockSchema.getDataSource()).thenReturn(mockDataSource);
        Mockito.when(mockDataSource.isServerVersionAtLeast(ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(false);
        Mockito.when(mockDataSource.getServerType()).thenReturn(server);
        Mockito.when(mockResults.getString("fmttype")).thenReturn("c");
        Mockito.when(mockResults.getString("urilocation")).thenReturn("gpfdist://filehost:8081/*.txt");
    }

    @Test
    public void createRelationOfClass_whenTableIsNotACloudberryTable_returnsInstanceOfPostgresTableBase() {
        Assert.assertEquals(CloudberryTable.class,
                server.createRelationOfClass(mockSchema, PostgreClass.RelKind.p, mockResults).getClass());
    }

    @Test
    public void createRelationOfClass_whenTableTypeIsRegularAndTableIsANonExternalCloudberryTable_returnsInstanceOfCloudberryTable()
            throws SQLException {
        Mockito.when(mockResults.getBoolean("is_ext_table")).thenReturn(false);
        Assert.assertEquals(CloudberryTable.class,
                server.createRelationOfClass(mockSchema, PostgreClass.RelKind.r, mockResults).getClass());
    }

    @Test
    public void createRelationOfClass_whenTableTypeIsRegularAndTableIsAnExternalCloudberryTable_returnsInstanceOfCloudberryExternalTable()
            throws SQLException {
        Mockito.when(mockResults.getBoolean("is_ext_table")).thenReturn(true);
        Assert.assertEquals(CloudberryExternalTable.class,
                server.createRelationOfClass(mockSchema, PostgreClass.RelKind.r, mockResults).getClass());
    }

    @Test
    public void readTableDDL_whenTableIsNotAnInstanceOfCloudberryExternalTable_delegatesDDLcreationToParentClass()
            throws DBException {
        String expectedDelegatedResultFromParentClass = null;
        CloudberryTable table = Mockito.mock(CloudberryTable.class);
        Assert.assertEquals(expectedDelegatedResultFromParentClass, server.readTableDDL(monitor, table));
    }

    @Test
    public void readTableDDL_whenTableIsAnInstanceOfCloudberryExternalTable_delegatesToGpGenerateDDL()
            throws DBException {
        CloudberryExternalTable table = Mockito.mock(CloudberryExternalTable.class);
        server.readTableDDL(monitor, table);
        Mockito.verify(table).generateDDL(monitor);
    }

    @Test
    public void configureDialect_shouldContainCloudberrySpecificKeywords() {
        PostgreDialect dialect = new PostgreDialect();

        server.configureDialect(dialect);

        Assert.assertTrue(!dialect.getMatchedKeywords("DISTRIBUTED").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("SEGMENT").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("REJECT").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("FORMAT").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("MASTER").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("WEB").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("WRITABLE").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("READABLE").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("ERRORS").isEmpty());
        Assert.assertTrue(!dialect.getMatchedKeywords("LOG").isEmpty());
    }
}