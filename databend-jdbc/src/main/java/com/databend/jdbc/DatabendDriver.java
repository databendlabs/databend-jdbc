package com.databend.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabendDriver extends NonRegisteringDatabendDriver {
    static {
        try {
            DriverManager.registerDriver(new DatabendDriver());
        } catch (SQLException e) {
            Logger.getLogger(DatabendDriver.class.getPackage().getName())
                    .log(Level.SEVERE, "Failed to register driver", e);
            throw new RuntimeException("Failed to register DatabendDriver", e);
        }
    }
}
