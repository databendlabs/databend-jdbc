package com.databend.jdbc;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
class ParamMarker {
    // Id / index of the param marker in the SQL statement
    int id;
    // Position in the SQL subStatement
    int position;
}
