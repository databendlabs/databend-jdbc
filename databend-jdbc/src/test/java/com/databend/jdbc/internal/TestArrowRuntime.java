package com.databend.jdbc.internal;

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
    public void testUnsupportedRuntimeIsRejected(String version) {
        // Pass the value explicitly; never mutate the JVM-wide version property in tests.
        Assert.assertFalse(ArrowRuntime.isSupported(version), version);
    }

    @DataProvider(name = "supportedOrUnknownVersions")
    public Object[][] supportedOrUnknownVersions() {
        return new Object[][] {{"11"}, {"17"}, {"25"}, {null}, {""}, {"unknown"}, {"1.unknown"}, {"17-ea"}};
    }

    @Test(groups = {"UNIT"}, dataProvider = "supportedOrUnknownVersions")
    public void testSupportedOrUnknownRuntimeIsPermitted(String version) {
        Assert.assertTrue(ArrowRuntime.isSupported(version), version);
    }

    @Test(groups = {"UNIT"})
    public void testUnsupportedMessageIsActionable() {
        Assert.assertTrue(ArrowRuntime.UNSUPPORTED_MESSAGE.contains("requires Java 11 or newer"));
        Assert.assertTrue(ArrowRuntime.UNSUPPORTED_MESSAGE.contains("query_result_format=json"));
    }

    @Test(groups = {"UNIT"})
    public void testRequireSupportedMatchesCurrentRuntime() {
        if (ArrowRuntime.isSupported()) {
            ArrowRuntime.requireSupported();
            return;
        }
        DatabendQueryException error = Assert.expectThrows(DatabendQueryException.class, ArrowRuntime::requireSupported);
        Assert.assertEquals(error.getMessage(), ArrowRuntime.UNSUPPORTED_MESSAGE);
    }
}
