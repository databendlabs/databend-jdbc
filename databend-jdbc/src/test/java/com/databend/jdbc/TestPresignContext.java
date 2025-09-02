package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.SQLException;

public class TestPresignContext {
    @Test(groups = {"UNIT"})
    public void TestPreisgnUrlBuild() {
        String presignSql = PresignContext.buildRequestSQL(PresignContext.PresignMethod.UPLOAD, "test_bucket", "test.csv");
        Assert.assertEquals(presignSql, "PRESIGN UPLOAD @test_bucket/test.csv");
        presignSql = PresignContext.buildRequestSQL(PresignContext.PresignMethod.DOWNLOAD, "test_bucket", "test.csv");
        Assert.assertEquals(presignSql, "PRESIGN DOWNLOAD @test_bucket/test.csv");
        presignSql = PresignContext.buildRequestSQL(PresignContext.PresignMethod.UPLOAD, null, "a/b/c/test.csv");
        Assert.assertEquals(presignSql, "PRESIGN UPLOAD @~/a/b/c/test.csv");
    }

    @Test(groups = {"IT"})
    public void TestGetPresignUrl() throws SQLException {
        DatabendConnection connection = (DatabendConnection) Utils.createConnection();
        PresignContext ctx = PresignContext.getPresignContext(connection, PresignContext.PresignMethod.UPLOAD, null, "test.csv");
        Assert.assertNotNull(ctx);
        Assert.assertNotNull(ctx.getUrl());
        Assert.assertNotNull(ctx.getHeaders());
    }

    @Test(groups = {"IT"})
    public void TestGetPresignUrlCase2() throws SQLException {
        try (DatabendConnection connection = (DatabendConnection) Utils.createConnection()) {
            String stageName = "test_stage";
            PresignContext.createStageIfNotExists(connection, stageName);
            PresignContext ctx = PresignContext.getPresignContext(connection, PresignContext.PresignMethod.UPLOAD, stageName, "a/b/d/test.csv");
            Assert.assertNotNull(ctx);
            Assert.assertNotNull(ctx.getUrl());
            Assert.assertNotNull(ctx.getHeaders());
        }
    }
}
