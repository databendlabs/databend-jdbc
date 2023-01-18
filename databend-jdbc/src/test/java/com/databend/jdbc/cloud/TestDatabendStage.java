package com.databend.jdbc.cloud;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestDatabendStage
{
    @Test(groups = {"Unit"})
    public void testDatabendStage()
    {
        DatabendStage stage = DatabendStage.builder()
                .stageName("stage_name")
                .path("path")
                .build();
        assertEquals(stage.getStageName(), "stage_name");
        assertEquals(stage.getPath(), "path");
        assertEquals(stage.toString(), "@stage_name/path");
    }

    @Test(groups = {"Unit"})
    public void testDatabendStageWithExternalLocation()
    {
        DatabendStage stage = DatabendStage.builder()
                .externalLocationS3(ExternalLocationS3.builder()
                        .setLocation("s3://mybucket/path/a/b/c")
                        .build())
                .build();
        assertEquals(stage.getExternalLocationS3().getLocation(), "s3://mybucket/path/a/b/c");
        assertEquals(stage.toString(), "'s3://mybucket/path/a/b/c' CONNECTION = (ENABLE_VIRTUAL_HOST_STYLE = 'false' )");
        stage = DatabendStage.builder().externalLocationS3(ExternalLocationS3.builder().setLocation("s3://mubucket/ab/b")
                .setAccessKeyId("asad").setSecretAccessKey("ancs").setRegion("us-east-2").setEndpointUrl("127.0.0.1:9001").build()).build();
        assertEquals(stage.getExternalLocationS3().getLocation(), "s3://mubucket/ab/b");
        assertEquals(stage.getExternalLocationS3().getAccessKeyId(), "asad");
        assertEquals(stage.getExternalLocationS3().getSecretAccessKey(), "ancs");
        assertEquals(stage.getExternalLocationS3().getEndpointUrl(), "127.0.0.1:9001");
        assertEquals(stage.getExternalLocationS3().getRegion(), "us-east-2");
        assertEquals(stage.toString(), "'s3://mubucket/ab/b' CONNECTION = (ENDPOINT_URL = '127.0.0.1:9001' ACCESS_KEY_ID = 'asad' SECRET_ACCESS_KEY = 'ancs' REGION = 'us-east-2' ENABLE_VIRTUAL_HOST_STYLE = 'false' )");
    }
}
