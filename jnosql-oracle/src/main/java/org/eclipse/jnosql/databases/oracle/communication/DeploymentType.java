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

/**
 * The {@code DeploymentType} enum represents different deployment options for software solutions.
 * It is used to categorize deployments as either "On-premises" or "Cloud."
 */
public enum DeploymentType {
    /**
     * Represents an "On-premises" deployment where software solutions are deployed and managed
     * within an organization's physical premises or data centers.
     */
    ON_PREMISES,

    /**
     * Represents a "Cloud" deployment where software solutions are delivered and managed
     * over the internet (the cloud) by third-party cloud providers.
     */
    CLOUD;

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
