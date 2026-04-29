package com.databend.jdbc;

import com.databend.jdbc.internal.query.StageAttachment;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestStageAttachment {
    @Test(groups = {"UNIT"})
    public void TestStageAttachment() {
        String uuid = "uuid/";
        String stagePrefix = "prefix/" + uuid;
        String fileName = "test";
        String stagePath = "@~/" + stagePrefix + fileName;
        StageAttachment attachment = new StageAttachment(stagePath, null, null);
        assertEquals("StageAttachment{location=@~/prefix/uuid/test, file_format_options={type=CSV}, copy_options=null}", attachment.toString());
    }
}
