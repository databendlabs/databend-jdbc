package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.exception.DatabendQueryException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestArrowRuntime {
    @DataProvider(name = "unsupportedVersions")
    public Object[][] unsupportedVersions() {
        return new Object[][] {{"1.8"}, {"8"}, {"9"}, {"10"}};
    }

    @Test(groups = {"UNIT"}, dataProvider = "unsupportedVersions")
    public void testUnsupportedRuntimeHasActionableError(String version) {
        DatabendQueryException error = Assert.expectThrows(DatabendQueryException.class,
                () -> RestQueryResultPages.requireArrowRuntime(version));
        Assert.assertTrue(error.getMessage().contains("requires Java 11 or newer"));
        Assert.assertTrue(error.getMessage().contains("query_result_format=json"));
    }

    @DataProvider(name = "supportedOrUnknownVersions")
    public Object[][] supportedOrUnknownVersions() {
        return new Object[][] {{"11"}, {"17"}, {"25"}, {null}, {""}, {"unknown"}, {"1.unknown"}, {"17-ea"}};
    }

    @Test(groups = {"UNIT"}, dataProvider = "supportedOrUnknownVersions")
    public void testSupportedOrUnknownRuntimeIsPermitted(String version) {
        // Pass the value explicitly; never mutate the JVM-wide version property in tests.
        RestQueryResultPages.requireArrowRuntime(version);
    }
}
