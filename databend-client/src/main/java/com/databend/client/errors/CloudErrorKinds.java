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

package com.databend.client.errors;

import static com.google.common.base.MoreObjects.toStringHelper;

// CloudErrorKinds is a list of error kinds that can be returned by the databend cloud service.
public enum CloudErrorKinds
{
    TENANT_NOT_FOUND("TenantNotFound", "Tenant not found, please check your tenant id", false),
    TENANT_BOOTSTRAP_FAILED("TenantBootstrapFailed", "tenant bootstrap failed", false),
    WAREHOUSE_NOT_FOUND("WarehouseNotFound", "warehouse not found", false),
    BAD_WAREHOUSE("BadWarehouse", "bad warehouse", false),
    WAREHOUSE_ALREADY_EXISTS("WarehouseAlreadyExists", "warehouse already exists", false),
    EMPTY_REQUEST("EmptyRequest", "empty request", false),
    BAD_REQUEST("BadRequest", "bad request", false),
    REQUIRE_TENANT_HEADER("TenantHeaderRequired", "X-DATABEND-TENANT is required", false),
    REQUIRE_PIPE_NAME("PipeNameRequired", "pipe name is required", false),
    REQUIRE_WAREHOUSE_HEADER("WarehouseHeaderRequired", "X-DATABENDCLOUD-WAREHOUSE is required", false),
    JWT_VERIFICATION_FAILED("JWTVerificationFailed", "JWT Verification Failed", false),
    PASSWORD_AUTH_FAILED("PasswordAuthFailed", "Please check your username and password", false),
    FORBIDDEN_ACCESS_USER("ForbiddenAccessUser", "Permission denied", false),
    ILLEGAL_HOST_NAME("IllegalHostName", "HostName is illegal", false),
    BAD_GATEWAY("BadGateway", "bad gateway", false),
    GATEWAY_TIMEOUT("GatewayTimeout", "gateway timeout", false),
    PROVISION_WAREHOUSE_TIMEOUT("ProvisionWarehouseTimeout", "provision warehouse timeout", true),
    UNEXPECTED("Unexpected", "unexpected", false),
    NOT_IMPLEMENTED("NotImplemented", "not implemented", false),
    HEALTH_CHECK_FAILED("HealthCheckFailed", "health check failed", false),
    BAD_TENANT("BadTenant", "bad tenant", false);

    private  final String kind;
    private  final String description;
    private  final boolean canRetry;

    CloudErrorKinds(String kind, String description, boolean canRetry) {
        this.kind = kind;
        this.description = description;
        this.canRetry = canRetry;
    }

    public static CloudErrorKinds tryGetErrorKind(String kind) {
        for (CloudErrorKinds errorKind : CloudErrorKinds.values()) {
            if (errorKind.getKind().equalsIgnoreCase(kind)) {
                return errorKind;
            }
        }
        return CloudErrorKinds.UNEXPECTED;
    }

    public String getKind() {
        return kind;
    }

    public String getDescription() {
        return description;
    }

    public boolean canRetry() {
        return canRetry;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("kind", kind)
                .add("description", description)
                .add("canRetry", canRetry)
                .toString();
    }
}
