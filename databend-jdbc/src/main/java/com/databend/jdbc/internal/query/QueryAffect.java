package com.databend.jdbc.internal.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = QueryAffect.Create.class, name = "Create"),
        @JsonSubTypes.Type(value = QueryAffect.UseDB.class, name = "UseDB"),
        @JsonSubTypes.Type(value = QueryAffect.ChangeSettings.class, name = "ChangeSettings")
})
public abstract class QueryAffect {
    public static class Create extends QueryAffect {
        private final String kind;
        private final String name;
        private final boolean success;

        @JsonCreator
        public Create(
                @JsonProperty("kind") String kind,
                @JsonProperty("name") String name,
                @JsonProperty("success") boolean success) {
            this.kind = kind;
            this.name = name;
            this.success = success;
        }

        @JsonProperty
        public String getKind() {
            return kind;
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public boolean isSuccess() {
            return success;
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("kind", kind).add("name", name).add("success", success).toString();
        }
    }

    public static class UseDB extends QueryAffect {
        private final String name;

        @JsonCreator
        public UseDB(@JsonProperty("name") String name) {
            this.name = name;
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("name", name).toString();
        }
    }

    public static class ChangeSettings extends QueryAffect {
        private final List<String> keys;
        private final List<String> values;
        private final List<Boolean> isGlobals;

        @JsonCreator
        public ChangeSettings(
                @JsonProperty("keys") List<String> keys,
                @JsonProperty("values") List<String> values,
                @JsonProperty("is_globals") List<Boolean> isGlobals) {
            this.keys = keys;
            this.values = values;
            this.isGlobals = isGlobals;
        }

        @JsonProperty
        public List<String> getKeys() {
            return keys;
        }

        @JsonProperty
        public List<String> getValues() {
            return values;
        }

        @JsonProperty
        public List<Boolean> getIsGlobals() {
            return isGlobals;
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("keys", keys).add("values", values).add("isGlobals", isGlobals).toString();
        }
    }
}
