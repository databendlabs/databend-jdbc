package com.databend.jdbc;

import com.databend.client.StageAttachment;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestStageAttachment {
    @Test(groups = {"UNIT"})
    public void TestStageAttachment() {
        String uuid = "uuid/";
        String stagePrefix = "prefix/" + uuid;
        String fileName = "test";
        String stagePath = "@~/" + stagePrefix + fileName;
        StageAttachment attachment = new StageAttachment.Builder().setLocation(stagePath)
                .build();
        assertEquals("StageAttachment{location=@~/prefix/uuid/test, file_format_options={type=CSV}, copy_options=null}", attachment.toString());

    }
}
