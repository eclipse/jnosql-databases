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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    public static Map<String, AttributeValue> toItem(UnaryOperator<String> entityNameResolver, DocumentEntity documentEntity) {
        UnaryOperator<String> resolver = Optional.ofNullable(entityNameResolver).orElse(UnaryOperator.identity());
        Map<String, Object> documentAttributes = getMap(resolver, documentEntity);
        return toItem(documentAttributes);
    }

    private static Map<String, AttributeValue> toItem(Map<String, Object> documentAttributes) {
        HashMap<String, AttributeValue> result = new HashMap<>();
        documentAttributes.forEach((attribute, value) -> result.put(attribute, toAttributeValue(value)));
        return result;
    }

    public static AttributeValue toAttributeValue(Object value) {
        if (value == null)
            return AttributeValue.builder().nul(true).build();
        if (value instanceof String str)
            return AttributeValue.builder().s(str).build();
        if (value instanceof Number number)
            return AttributeValue.builder().n(String.valueOf(number)).build();
        if (value instanceof Boolean bool)
            return AttributeValue.builder().bool(bool).build();
        if (value instanceof List<?> list)
            return AttributeValue.builder().l(list.stream().filter(Objects::nonNull)
                    .map(DocumentEntityConverter::toAttributeValue).toList()).build();
        if (value instanceof Map<?, ?> mapValue) {
            HashMap<String, AttributeValue> values = new HashMap<>();
            mapValue.forEach((k, v) -> values.put(String.valueOf(k), toAttributeValue(v)));
            return AttributeValue.builder().m(values).build();
        }
        if (value instanceof byte[] data) {
            return AttributeValue.builder().b(SdkBytes.fromByteArray(data)).build();
        }
        if (value instanceof ByteBuffer byteBuffer) {
            return AttributeValue.builder().b(SdkBytes.fromByteBuffer(byteBuffer)).build();
        }
        if (value instanceof InputStream input) {
            return AttributeValue.builder().b(SdkBytes.fromInputStream(input)).build();
        }
        return AttributeValue.builder().s(String.valueOf(value)).build();
    }


    public static DocumentEntity toDocumentEntity(UnaryOperator<String> entityNameResolver, Map<String, AttributeValue> item) {
        if (item == null) {
            return null;
        }
        if (item.isEmpty()) {
            return null;
        }
        UnaryOperator<String> resolver = Optional.ofNullable(entityNameResolver).orElse(UnaryOperator.identity());
        String entityAttribute = resolver.apply(ENTITY);
        var entityName = item.containsKey(entityAttribute) ? item.get(entityAttribute).s() : entityAttribute;
        List<Document> documents = item.entrySet()
                .stream()
                .filter(entry -> !Objects.equals(entityAttribute, entry.getKey()))
                .map(entry -> Document.of(entry.getKey(), convertValue(entry.getValue())))
                .toList();
        return DocumentEntity.of(entityName, documents);
    }
}
