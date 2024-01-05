/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.communication;

import jakarta.json.bind.Jsonb;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.driver.JsonbSupplier;
import org.eclipse.jnosql.communication.driver.ValueUtil;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

class DocumentEntityConverter {

    static final String ENTITY = "@entity";
    static final String ID = "_id";
    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();

    private DocumentEntityConverter() {
    }

    static DocumentEntity toDocumentEntity(UnaryOperator<String> entityNameResolver, EnhancedDocument enhancedDocument) {
        if (enhancedDocument == null) {
            return null;
        }
        if (enhancedDocument.toMap().isEmpty()) {
            return null;
        }
        UnaryOperator<String> resolver = Optional.ofNullable(entityNameResolver).orElse(UnaryOperator.identity());
        String entityAttribute = resolver.apply(ENTITY);
        Map<String, AttributeValue> map = enhancedDocument.toMap();
        var entityName = map.containsKey(entityAttribute) ? map.get(entityAttribute).s() : entityAttribute;
        List<Document> documents = map.entrySet()
                .stream()
                .filter(entry -> !Objects.equals(entityAttribute, entry.getKey()))
                .map(entry -> Document.of(entry.getKey(), convertValue(entry.getValue())))
                .toList();
        return DocumentEntity.of(entityName, documents);
    }

    private static Object convertValue(Object value) {
        if (value instanceof AttributeValue attributeValue) {
            switch (attributeValue.type()) {
                case S:
                    return attributeValue.s();
                case N:
                    return Double.valueOf(attributeValue.n());
                case B:
                    return attributeValue.b().asByteArray();
                case SS:
                    return attributeValue.ss();
                case NS:
                    return attributeValue.ns().stream().map(Double::valueOf).toList();
                case BS:
                    return attributeValue.bs().stream().map(SdkBytes::asByteArray).toList();
                case L:
                    return attributeValue.l().stream().map(DocumentEntityConverter::convertValue).toList();
                case M:
                    return attributeValue.m().entrySet().stream().map(e -> Document.of(e.getKey(), convertValue(e.getValue()))).toList();
                case NUL:
                    return null;
                case BOOL:
                    return attributeValue.bool();
                case UNKNOWN_TO_SDK_VERSION:
                default:
                    return null; // map type
            }
        }
        return value;
    }

    static EnhancedDocument toEnhancedDocument(UnaryOperator<String> entityNameResolver, DocumentEntity documentEntity) {
        UnaryOperator<String> resolver = Optional.ofNullable(entityNameResolver).orElse(UnaryOperator.identity());
        Map<String, Object> documentAsMap = getMap(resolver, documentEntity);
        return EnhancedDocument.builder()
                .json(JSONB.toJson(documentAsMap))
                .build();
    }

    static Map<String, Object> getMap(UnaryOperator<String> entityNameResolver, DocumentEntity entity) {
        var nameResolver = Optional.ofNullable(entityNameResolver).orElse(UnaryOperator.identity());
        Map<String, Object> jsonObject = new HashMap<>();
        entity.documents().forEach(feedJSON(jsonObject));
        jsonObject.put(Optional.ofNullable(nameResolver.apply(ENTITY)).orElse(ENTITY), entity.name());
        return jsonObject;
    }

    private static Consumer<Document> feedJSON(Map<String, Object> jsonObject) {
        return d -> {
            Object value = ValueUtil.convert(d.value());
            if (value instanceof Document) {
                Document subDocument = Document.class.cast(value);
                jsonObject.put(d.name(), singletonMap(subDocument.name(), subDocument.get()));
            } else if (isSudDocument(value)) {
                Map<String, Object> subDocument = getMap(value);
                jsonObject.put(d.name(), subDocument);
            } else if (isSudDocumentList(value)) {
                jsonObject.put(d.name(), StreamSupport.stream(Iterable.class.cast(value).spliterator(), false)
                        .map(DocumentEntityConverter::getMap).collect(toList()));
            } else {
                jsonObject.put(d.name(), value);
            }
        };
    }

    private static Map<String, Object> getMap(Object value) {
        Map<String, Object> subDocument = new HashMap<>();
        StreamSupport.stream(Iterable.class.cast(value).spliterator(),
                false).forEach(feedJSON(subDocument));
        return subDocument;
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(org.eclipse.jnosql.communication.document.Document.class::isInstance);
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }
}
