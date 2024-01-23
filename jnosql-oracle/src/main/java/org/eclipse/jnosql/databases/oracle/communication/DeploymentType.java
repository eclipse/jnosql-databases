/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v1.0
 *   and Apache License v2.0 which accompanies this distribution.
 *   The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 *   and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 *   You may elect to redistribute this code under either of these licenses.
 *
 *   Contributors:
 *
 *   Otavio Santana
 */
package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import org.eclipse.jnosql.communication.Configurations;
import org.eclipse.jnosql.communication.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * The {@code DeploymentType} enum represents different deployment options for software solutions.
 * It is used to categorize deployments as either "On-premises" or "Cloud."
 */
public enum DeploymentType implements Function<Settings, Optional<AuthorizationProvider>> {
    /**
     * Represents an "On-premises" deployment where software solutions are deployed and managed
     * within an organization's physical premises or data centers.
     */
    ON_PREMISES{
        @Override
        public Optional<AuthorizationProvider> apply(Settings settings) {
            String user = settings.get(List.of(OracleNoSQLConfigurations.USER.get(), Configurations.USER.get()))
                    .map(Object::toString).orElse(null);
            String password = settings.get(List.of(OracleNoSQLConfigurations.PASSWORD.get(), Configurations.PASSWORD.get()))
                    .map(Object::toString).orElse(null);
            if (user != null && password != null) {
               return Optional.of(new StoreAccessTokenProvider(user, password.toCharArray()));
            } else {
                return Optional.of(new StoreAccessTokenProvider());
            }

        }
    },

    /**
     * Represents a "Cloud" deployment where software solutions are delivered and managed
     * over the internet (the cloud) by Oracle cloud provider.
     */
    CLOUD{
        @Override
        public Optional<AuthorizationProvider> apply(Settings settings) {

            String user = settings.get(List.of(OracleNoSQLConfigurations.USER.get(), Configurations.USER.get()))
                    .map(Object::toString).orElse(null);
            char[] password = settings.get(List.of(OracleNoSQLConfigurations.PASSWORD.get(), Configurations.PASSWORD.get()))
                    .map(Object::toString).map(String::toCharArray).orElse(new char[0]);
            String tenantId = settings.get(OracleNoSQLConfigurations.TENANT, String.class).orElse(null);
            String fingerprint= settings.get(OracleNoSQLConfigurations.FINGERPRINT, String.class).orElse(null);
            String privateKey = settings.get(OracleNoSQLConfigurations.PRIVATE_KEY, String.class).orElse(null);

            if(user != null && password.length > 0 && tenantId != null && fingerprint != null && privateKey != null) {
                return Optional.of(new SignatureProvider(tenantId, user, fingerprint, privateKey, password));
            }

            try {
                return Optional.of(new SignatureProvider());
            } catch (IOException e) {
                throw new OracleNoSQLException("Error to load configuration to Oracle Cloud at Oracle NoSQL", e);
            }

        }
    };


    /**
     * Parses a string representation of a deployment type into the {@link DeploymentType} enum,
     * with a default value of {@link DeploymentType#ON_PREMISES} for invalid or null input.
     *
     * @param value the string representation of the deployment type (case-insensitive)
     * @return the corresponding {@link DeploymentType} enum value, or {@link DeploymentType#ON_PREMISES}
     *         if the input is invalid or null
     *
     */
    public static DeploymentType parse(String value) {
        try {
            return DeploymentType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException | NullPointerException e) {
            return DeploymentType.ON_PREMISES;
        }
    }

}
