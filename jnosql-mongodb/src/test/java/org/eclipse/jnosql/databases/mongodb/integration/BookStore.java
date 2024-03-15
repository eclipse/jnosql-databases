package org.eclipse.jnosql.databases.mongodb.integration;

import jakarta.data.repository.Repository;
import org.eclipse.jnosql.mapping.NoSQLRepository;

@Repository
public interface BookStore extends NoSQLRepository<BookOrder, String> {
}
