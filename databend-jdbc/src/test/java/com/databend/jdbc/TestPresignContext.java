package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestPresignContext {
    private Connection createConnection()
            throws SQLException {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    @Test(groups = {"Unit"})
    public void TestPreisgnUrlBuild() {
        String presignSql = PresignContext.buildRequestSQL(PresignContext.PresignMethod.UPLOAD, "test_bucket", "test.csv");
        Assert.assertEquals(presignSql, "PRESIGN UPLOAD @test_bucket/test.csv");
        presignSql = PresignContext.buildRequestSQL(PresignContext.PresignMethod.DOWNLOAD, "test_bucket", "test.csv");
        Assert.assertEquals(presignSql, "PRESIGN DOWNLOAD @test_bucket/test.csv");
        presignSql = PresignContext.buildRequestSQL(PresignContext.PresignMethod.UPLOAD, null, "a/b/c/test.csv");
        Assert.assertEquals(presignSql, "PRESIGN UPLOAD @~/a/b/c/test.csv");
    }

    @Test(groups = {"IT"})
    public void TestGetPresignUrl() {
        try {
            DatabendConnection connection = (DatabendConnection) createConnection();
            PresignContext ctx = PresignContext.getPresignContext(connection, PresignContext.PresignMethod.UPLOAD, null, "test.csv");
            Assert.assertNotNull(ctx);
            Assert.assertNotNull(ctx.getUrl());
            Assert.assertNotNull(ctx.getHeaders());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = {"IT"})
    public void TestGetPresignUrlCase2() {
        try {
            DatabendConnection connection = (DatabendConnection) createConnection();
            String stageName = "test_stage";
            PresignContext.createStageIfNotExists(connection, stageName);
            PresignContext ctx = PresignContext.getPresignContext(connection, PresignContext.PresignMethod.UPLOAD, stageName, "a/b/d/test.csv");
            Assert.assertNotNull(ctx);
            Assert.assertNotNull(ctx.getUrl());
            Assert.assertNotNull(ctx.getHeaders());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
