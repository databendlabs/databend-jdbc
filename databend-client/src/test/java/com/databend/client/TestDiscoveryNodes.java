package com.databend.client;

import com.databend.client.errors.QueryErrors;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static com.databend.client.JsonCodec.jsonCodec;

public class TestDiscoveryNodes {
    private static final JsonCodec<DiscoveryResponseCodec.DiscoveryResponse> QUERY_RESULTS_CODEC = jsonCodec(DiscoveryResponseCodec.DiscoveryResponse.class);

    @Test(groups = {"unit"})
    public void testDecodeValidJsonArray() throws JsonProcessingException {
        String json = "[{\"address\":\"127.0.0.1:8003\"},{\"address\":\"127.0.0.1:8002\"},{\"address\":\"sfgsjsdhida:111\"}]";
        DiscoveryResponseCodec.DiscoveryResponse errorResponse = QUERY_RESULTS_CODEC.fromJson(json);
        List<DiscoveryNode> nodesList = errorResponse.getNodes();
        Assert.assertEquals(nodesList.size(), 3);
        Assert.assertEquals(nodesList.get(0).getAddress(), "127.0.0.1:8003");
        Assert.assertEquals(nodesList.get(1).getAddress(), "127.0.0.1:8002");
        Assert.assertEquals(nodesList.get(2).getAddress(), "sfgsjsdhida:111");
    }

    @Test(groups = {"unit"})
    public void testQueryError() throws JsonProcessingException {
        String json = "{\"error\":{\"code\":404,\"message\":\"not found\"}}";
        DiscoveryResponseCodec.DiscoveryResponse errorResponse = QUERY_RESULTS_CODEC.fromJson(json);
        QueryErrors error = errorResponse.getError();
        Assert.assertEquals(error.getMessage(), "not found");
        Assert.assertEquals(error.getCode(), 404);
    }

    // test empty array
    @Test(groups = {"unit"})
    public void testEmptyArray() throws JsonProcessingException {
        String json = "[]";
        DiscoveryResponseCodec.DiscoveryResponse errorResponse = QUERY_RESULTS_CODEC.fromJson(json);
        List<DiscoveryNode> nodesList = errorResponse.getNodes();
        Assert.assertEquals(nodesList.size(), 0);
    }


}
