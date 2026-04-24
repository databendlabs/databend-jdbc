package com.databend.jdbc.internal.binding;

public class ParamMarker {
    // Id / index of the param marker in the SQL statement
    private final int id;
    // Position in the SQL subStatement
    private final int position;

    public ParamMarker(int id, int position) {
        this.id = id;
        this.position = position;
    }

    public int getId() {
        return id;
    }

    public int getPosition() {
        return position;
    }
}
