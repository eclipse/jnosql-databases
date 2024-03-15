package org.eclipse.jnosql.databases.mongodb.integration;

import jakarta.nosql.Column;
import jakarta.nosql.Entity;

@Entity
public record BookOrderItem(@Column Book book, @Column Integer quantity) {
}
