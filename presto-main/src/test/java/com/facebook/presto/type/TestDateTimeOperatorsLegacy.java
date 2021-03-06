/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.type;

import com.facebook.presto.Session;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.HOURS;

public class TestDateTimeOperatorsLegacy extends TestDateTimeOperatorsBase
{
    public TestDateTimeOperatorsLegacy()
    {
        super(
                testSessionBuilder()
                        .setTimeZoneKey(TIME_ZONE_KEY)
                        .setSystemProperty("legacy_timestamp", "true")
                        .build()
        );
    }

    @Test
    public void testDaylightTimeSavingSwitchCrossingIsApplied()
    {
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '1' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 1, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '2' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        // we need to manipulate millis directly here because 2 am has two representations in out time zone, and we need the second one
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '3' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis() + HOURS.toMillis(3), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '4' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 3, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '4' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 02:05' - INTERVAL '2' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 01:05' - INTERVAL '1' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '1' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis() + HOURS.toMillis(3), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '2' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testTimeZoneGapIsApplied()
    {
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '1' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 1, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '2' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 3, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '3' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 4, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-03-31 04:05' - INTERVAL '3' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 03:05' - INTERVAL '2' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 01:05' - INTERVAL '1' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    private String valueFromLiteral(String literal)
    {
        Pattern p = Pattern.compile("'(.*)'");
        Matcher m = p.matcher(literal);
        m.find();
        return m.group(1);
    }

    private void testTimeRepresentationOnDate(DateTime date, String timeLiteral, Type expectedType, Object expected)
    {
        Session localSession = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("America/Los_Angeles"))
                .setStartTime(date.getMillis())
                .setSystemProperty("legacy_timestamp", "true")
                .build();

        FunctionAssertions localAssertions = new FunctionAssertions(localSession);
        localAssertions.assertFunction(timeLiteral, expectedType, expected);
        localAssertions.assertFunctionString(timeLiteral, expectedType, valueFromLiteral(timeLiteral));
    }

    @Test
    public void testTimeWithTimeZoneRepresentation()
    {
        // PST -> PDT date
        testTimeRepresentationOnDate(new DateTime(2017, 3, 12, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 3, 12, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PDT -> PST date
        testTimeRepresentationOnDate(new DateTime(2017, 10, 4, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 10, 4, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PDT date
        testTimeRepresentationOnDate(new DateTime(2017, 6, 6, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 6, 6, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PST date
        testTimeRepresentationOnDate(new DateTime(2017, 11, 1, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 11, 1, 10, 0, 0, 0, DateTimeZone.UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));
    }

    @Test
    public void testTimeRepresentation()
    {
        // PST -> PDT date
        testTimeRepresentationOnDate(new DateTime(2017, 3, 12, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 3, 12, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PDT -> PST date
        testTimeRepresentationOnDate(new DateTime(2017, 10, 4, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 10, 4, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PDT date
        testTimeRepresentationOnDate(new DateTime(2017, 6, 6, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 6, 6, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PST date
        testTimeRepresentationOnDate(new DateTime(2017, 11, 1, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(new DateTime(2017, 11, 1, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(41400000, getTimeZoneKey("America/Los_Angeles")));
    }
}
