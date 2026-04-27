package com.databend.jdbc;

import com.databend.jdbc.internal.data.DatabendRawType;
import com.databend.jdbc.internal.query.QueryRowField;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TestTypedResultSetValues {
    @Test(groups = {"UNIT"})
    public void testIntervalValueRoundTripsThroughResultSet() throws Exception {
        List<QueryRowField> schema = Collections.singletonList(new QueryRowField("a", new DatabendRawType("Interval")));
        DatabendUnboundQueryResultSet resultSet = new DatabendUnboundQueryResultSet(Optional.empty(), schema,
                Collections.singletonList(Collections.<Object>singletonList(new IntervalValue(1, (2 * 3600L + 3 * 60L + 4L) * 1_000_000L))).iterator());

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(resultSet.getString(1), "1 day 2:03:04");
        Assert.assertEquals(resultSet.getObject(1), Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4));
    }

    @Test(groups = {"UNIT"})
    public void testTypedTimestampValueUsesDirectObjectConversion() throws Exception {
        Timestamp timestamp = Timestamp.from(Instant.parse("2024-04-16T12:34:56.789Z"));
        List<QueryRowField> schema = Arrays.asList(
                new QueryRowField("ts", new DatabendRawType("Timestamp")),
                new QueryRowField("d", new DatabendRawType("Date"))
        );
        DatabendUnboundQueryResultSet resultSet = new DatabendUnboundQueryResultSet(Optional.empty(), schema,
                Collections.singletonList(Arrays.<Object>asList(timestamp, java.sql.Date.valueOf("2024-04-16"))).iterator());

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(resultSet.getTimestamp(1), timestamp);
        Assert.assertEquals(resultSet.getObject(1, Instant.class), timestamp.toInstant());
        Assert.assertEquals(resultSet.getObject(2, java.time.LocalDate.class), java.time.LocalDate.of(2024, 4, 16));
    }
}
