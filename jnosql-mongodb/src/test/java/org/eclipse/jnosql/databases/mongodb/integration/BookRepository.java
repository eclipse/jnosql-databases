package org.eclipse.jnosql.databases.mongodb.integration;

import jakarta.data.repository.PageableRepository;
import jakarta.data.repository.Repository;

@Repository
public interface BookRepository extends PageableRepository<Book,String> {
}
