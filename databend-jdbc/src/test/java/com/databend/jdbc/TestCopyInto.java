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
        DatabendStage s = DatabendStage.builder().stageName("~").path("a/b/c").build();
        String sql = DatabendConnection.getCopyIntoSql("db1", DatabendCopyParams.builder().setDatabendStage(s).setDatabaseTableName("tb1").build());
        assertEquals(sql.trim(), "COPY INTO db1.tb1 FROM @~/a/b/c FILE_FORMAT = ( type = 'CSV' )");
        sql = DatabendConnection.getCopyIntoSql(null, DatabendCopyParams.builder().setDatabendStage(s).setDatabaseTableName("tb1").build());
        assertEquals(sql.trim(), "COPY INTO tb1 FROM @~/a/b/c FILE_FORMAT = ( type = 'CSV' )");

    }
}
