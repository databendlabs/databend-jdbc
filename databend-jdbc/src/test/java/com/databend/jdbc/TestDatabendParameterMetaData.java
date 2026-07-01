package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;


public class TestDatabendParameterMetaData {

    @BeforeTest(groups = "IT")
    public void setUp()
            throws SQLException {
        // create table
        Connection c = Utils.createConnection();
        System.out.println("-----------------");
        System.out.println("drop all existing test table");
    }


    @Test(groups = "IT")
    public void testGetParameterMetaData() throws SQLException {
        try (Connection conn = Utils.createConnection();
             PreparedStatement emptyPs = conn.prepareStatement("select 1");
             PreparedStatement inputPs = conn.prepareStatement(
                     "insert into non_existing_table (col2, col3, col1) values (?, ?, ?)");
             PreparedStatement sqlPs = conn.prepareStatement("insert into test_table (a, b, c) values (?,?,?)")) {
            Assert.assertEquals(emptyPs.getParameterMetaData().getParameterCount(), 0);

            for (PreparedStatement ps : new PreparedStatement[]{inputPs, sqlPs}) {
                Assert.assertNotNull(ps.getParameterMetaData());
                Assert.assertTrue(ps.getParameterMetaData() == ps.getParameterMetaData(),
                        "parameter mete data should be singleton");
                Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 3);
                Assert.assertEquals(ps.getParameterMetaData().getParameterMode(3), ParameterMetaData.parameterModeIn);
                Assert.assertEquals(ps.getParameterMetaData().getParameterType(3), Types.OTHER);
                Assert.assertEquals(ps.getParameterMetaData().getPrecision(3), 0);
                Assert.assertEquals(ps.getParameterMetaData().getScale(3), 0);
                Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(3), Object.class.getName());
                Assert.assertEquals(ps.getParameterMetaData().getParameterTypeName(3), "<unknown>");
            }
        }

        try (Connection conn = Utils.createConnection();
             PreparedStatement ps = conn.prepareStatement("insert into test_table (a, b) values (?,?)")) {
            Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 2);
            Assert.assertEquals(ps.getParameterMetaData().getParameterMode(2), ParameterMetaData.parameterModeIn);
            Assert.assertEquals(ps.getParameterMetaData().getParameterType(2), Types.OTHER);
            Assert.assertEquals(ps.getParameterMetaData().getPrecision(2), 0);
            Assert.assertEquals(ps.getParameterMetaData().getScale(2), 0);
            Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(2), Object.class.getName());
            Assert.assertEquals(ps.getParameterMetaData().getParameterTypeName(2), "<unknown>");
        }

        try (Connection conn = Utils.createConnection();
             PreparedStatement ps = conn.prepareStatement("insert into test_table (a, b) values")) {
            Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 2);
            Assert.assertEquals(ps.getParameterMetaData().getParameterMode(2), ParameterMetaData.parameterModeIn);
            Assert.assertEquals(ps.getParameterMetaData().getParameterType(2), Types.OTHER);
            Assert.assertEquals(ps.getParameterMetaData().getPrecision(2), 0);
            Assert.assertEquals(ps.getParameterMetaData().getScale(2), 0);
            Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(2), Object.class.getName());
            Assert.assertEquals(ps.getParameterMetaData().getParameterTypeName(2), "<unknown>");
        }
    }
}
