package com.databend.jdbc;


import java.util.Map;

class LoginRequest {
    public String database;
    public Map<String, String> settings;
}
