package org.eclipse.jnosql.databases.mongodb.integration;

import jakarta.nosql.Column;
import jakarta.nosql.Entity;
import jakarta.nosql.Id;

import java.util.List;

@Entity
public record BookOrder(@Id String id, @Column List<BookOrderItem> items){
}
