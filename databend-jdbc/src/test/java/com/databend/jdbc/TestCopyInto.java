package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendStage;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestCopyInto {
    @Test(groups = {"UNIT"})
    public void TestGenSQL() {
        DatabendStage s = DatabendStage.builder().stageName("~").path("a/b/c").build();
        List<String> files = new ArrayList<>();
        files.add("file.csv");
        String sql = DatabendConnectionImpl.getCopyIntoSql("db1", DatabendCopyParams.builder().setFiles(files).setDatabendStage(s).setDatabaseTableName("tb1").build());
        assertEquals(sql.trim(), "COPY INTO db1.tb1 FROM @~/a/b/c FILES = ('file.csv') FILE_FORMAT = ( type = 'CSV' )");
        sql = DatabendConnectionImpl.getCopyIntoSql(null, DatabendCopyParams.builder().setDatabendStage(s).setDatabaseTableName("tb1").build());
        assertEquals(sql.trim(), "COPY INTO tb1 FROM @~/a/b/c FILE_FORMAT = ( type = 'CSV' )");
    }
}
