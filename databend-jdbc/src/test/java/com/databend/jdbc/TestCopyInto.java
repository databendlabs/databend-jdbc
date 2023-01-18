package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendStage;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestCopyInto
{
    @Test(groups = {"Unit"})
    public void TestParseSql()
    {
        String sql = DatabendConnection.getCopyIntoSql("db1", "tb1", DatabendStage.builder().stageName("~").path("a/b/c").build(), DatabendCopyParams.builder().build());
        assertEquals(sql.trim(), "COPY INTO db1.tb1 FROM @~/a/b/c FILE_FORMAT = ( type = 'CSV' )");
        sql = DatabendConnection.getCopyIntoSql(null, "tb1", DatabendStage.builder().stageName("~").path("a/b/c").build(), DatabendCopyParams.builder().build());
        assertEquals(sql.trim(), "COPY INTO tb1 FROM @~/a/b/c FILE_FORMAT = ( type = 'CSV' )");

    }
}
