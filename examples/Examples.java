class Examples {
    private Connection createConnection()
            throws SQLException {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    public static void main(String[] args) {
        // insert into with PreparedStatement
        String sql = "insert into test_prepare_statement values (?,?)";
        Connection conn = createConnection();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setString(2, "b");
            statement.addBatch();
            int[] result = statement.executeBatch();
            System.out.println(result);
        }
        // update with PreparedStatement
        String updateSQL = "update test_prepare_statement set b = ? where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
            statement.setInt(2, 1);
            // Attention: now setString(1, "c") will throw exception, need to setString(1, "'c'")
            statement.setString(1, "'c'");
            int result = statement.executeUpdate();
            System.out.println(result);
        }

        // executeQuery and return ResultSet
        ResultSet r = conn.createStatement().executeQuery("select * from test_prepare_statement");
        while (r.next()) {
            System.out.println(r.getInt(1));
            System.out.println(r.getString(2));
        }

        // replace into with PreparedStatement
        String replaceIntoSQL = "replace into test_prepare_statement on(a) values (?,?)";
        try (PreparedStatement statement = conn.prepareStatement(replaceIntoSQL)) {
            statement.setInt(1, 1);
            statement.setString(2, "d");
            statement.addBatch();
            int[] result = statement.executeBatch();
        }
        ResultSet r2 = conn.createStatement().executeQuery("select * from test_prepare_statement");
        while (r2.next()) {
            System.out.println(r2.getInt(1));
            System.out.println(r2.getString(2));
        }
        // delete with PreparedStatement
        String deleteSQL = "delete from test_prepare_statement where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(deleteSQL)) {
            statement.setInt(1, 1);
            int result = statement.executeUpdate();
            System.out.println(result);
        }
        ResultSet r3 = conn.createStatement().executeQuery("select * from test_prepare_statement");
//        Assert.assertEquals(0, r3.getRow());
        while (r3.next()) {
            // noting print
            System.out.println(r3.getInt(1));
            System.out.println(r3.getString(2));
        }
    }
}
}
