package com.databend.jdbc;

import com.databend.client.StageAttachment;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class TestStageAttachment {
    @Test(groups = {"Unit"})
    public void TestStageAttachment(){
        String uuid = "uuid/";
        String stagePrefix = "prefix/"+uuid;
        String fileName = "test";
        String stagePath = "@~/" + stagePrefix + fileName;
        StageAttachment attachment = new StageAttachment.Builder().setLocation(stagePath)
                .setFileFormat(StageAttachment.defaultFileFormat)
                .build();
        assertEquals("StageAttachment{location=@~/prefix/uuid/test, file_format=( type = CSV ), file_format_options=null, copy_options=null}",attachment.toString());

    }
}
