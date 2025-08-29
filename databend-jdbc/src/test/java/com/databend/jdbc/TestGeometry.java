package com.databend.jdbc;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestGeometry {
    @Test(groups = {"IT"})
    public void testSelectGeometry() throws SQLException, ParseException {
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()
         ) {
            statement.execute("set enable_geo_create_table=1");
            statement.execute("CREATE or replace table cities ( id INT, name VARCHAR NOT NULL, location GEOMETRY);");
            statement.execute("INSERT INTO cities (id, name, location) VALUES (1, 'New York', 'POINT (-73.935242 40.73061))');");
            statement.execute("INSERT INTO cities (id, name, location) VALUES (2, 'Null', null);");
            try (ResultSet r = statement.executeQuery("select location from cities order by id")) {
                r.next();
                Assert.assertEquals("{\"type\": \"Point\", \"coordinates\": [-73.935242,40.73061]}", r.getObject(1));
                r.next();
                Assert.assertNull(r.getObject(1));
            }

            // set geometry_output_format to wkb
            connection.createStatement().execute("set geometry_output_format='WKB'");
            try (ResultSet r = statement.executeQuery("select location from cities order by id")) {
                r.next();
                byte[] wkb = r.getBytes(1);
                WKBReader wkbReader = new WKBReader();
                Geometry geometry = wkbReader.read(wkb);
                Assert.assertEquals("POINT (-73.935242 40.73061)", geometry.toText());
            }
        }
    }
}
