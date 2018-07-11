package org.jnosql.diana.couchdb.document;

final class CouchDBConstant {


    static final String ID = "_id";
    static final String REV = "_rev";
    static final String REV_RESPONSE = "rev";
    static final String ID_RESPONSE = "id";
    static final String ENTITY = "@entity";

    static final String ALL_DBS = "_all_dbs";
    static final String TOTAL_ROWS_RESPONSE = "total_rows";
    static final String REV_HEADER = "If-Match";
    static final String FIND = "/_find";
    static final String DOCS_RESPONSE = "docs";

    static final String COUNT = "/_all_docs?limit=0";
    public static final String BOOKMARK = "bookmark";

    private CouchDBConstant() {
    }
}
