package com.databend.jdbc;

import com.databend.client.data.DatabendDataType;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;


public class TestDatabendParameterMetaData {

    @BeforeTest
    public void setUp()
            throws SQLException {
        // create table
        Connection c = Utils.createConnection();
        System.out.println("-----------------");
        System.out.println("drop all existing test table");
    }


    @Test(groups = "integration")
    public void testGetParameterMetaData() throws SQLException {
        try (Connection conn = Utils.createConnection();
             PreparedStatement emptyPs = conn.prepareStatement("select 1");
             // If you want to use ps.getParameterMetaData().* methods, you need to use a valid sql such as
             // insert into table_name (col1 type1, col2 typ2, col3 type3) values (?, ?, ?)
             PreparedStatement inputPs = conn.prepareStatement(
                     "insert into non_existing_table ('col2 String, col3 Int8, col1 String') values (?, ?, ?)");
             PreparedStatement sqlPs = conn.prepareStatement("insert into test_table (a int, b int, c string) values (?,?,?)");) {
            Assert.assertEquals(emptyPs.getParameterMetaData().getParameterCount(), 0);

            for (PreparedStatement ps : new PreparedStatement[]{inputPs, sqlPs}) {
                Assert.assertNotNull(ps.getParameterMetaData());
                Assert.assertTrue(ps.getParameterMetaData() == ps.getParameterMetaData(),
                        "parameter mete data should be singleton");
                Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 3);
                Assert.assertEquals(ps.getParameterMetaData().getParameterMode(3), ParameterMetaData.parameterModeIn);
                Assert.assertEquals(ps.getParameterMetaData().getParameterType(3), Types.VARCHAR);
                Assert.assertEquals(ps.getParameterMetaData().getPrecision(3), 1024 * 1024 * 1024);
                Assert.assertEquals(ps.getParameterMetaData().getScale(3), 0);
                Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(3), String.class.getName());
                Assert.assertEquals(ps.getParameterMetaData().getParameterTypeName(3), DatabendDataType.STRING.name().toLowerCase());
            }
        }

        try (Connection conn = Utils.createConnection();
             PreparedStatement ps = conn.prepareStatement("insert into test_table (a int, b int) values (?,?)");) {
            Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 2);
            Assert.assertEquals(ps.getParameterMetaData().getParameterMode(2), ParameterMetaData.parameterModeIn);
            Assert.assertEquals(ps.getParameterMetaData().getParameterType(2), Types.INTEGER);
            Assert.assertEquals(ps.getParameterMetaData().getPrecision(2), 10);
            Assert.assertEquals(ps.getParameterMetaData().getScale(2), 0);
            Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(2), Integer.class.getName());
            Assert.assertEquals(ps.getParameterMetaData().getParameterTypeName(2), DatabendDataType.INT_32.getDisplayName().toLowerCase());
        }

        try (Connection conn = Utils.createConnection();
             PreparedStatement ps = conn.prepareStatement("insert into test_table (a int, b VARIANT) values (?,?)");) {
            Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 2);
            Assert.assertEquals(ps.getParameterMetaData().getParameterMode(2), ParameterMetaData.parameterModeIn);
            Assert.assertEquals(ps.getParameterMetaData().getParameterType(2), Types.VARCHAR);
            Assert.assertEquals(ps.getParameterMetaData().getPrecision(2), 0);
            Assert.assertEquals(ps.getParameterMetaData().getScale(2), 0);
            Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(2), String.class.getName());
            Assert.assertEquals(ps.getParameterMetaData().getParameterTypeName(2), DatabendDataType.VARIANT.getDisplayName().toLowerCase());
        }
    }
}
