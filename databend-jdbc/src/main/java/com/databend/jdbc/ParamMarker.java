package com.databend.jdbc;
import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class ParamMarker {
    int id; // Id / index of the param marker in the SQL statement
    int position; // Position in the SQL subStatement
}
