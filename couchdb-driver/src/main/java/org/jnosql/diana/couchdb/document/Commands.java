package org.jnosql.diana.couchdb.document;

enum Commands {

    ALL_DBS("_all_dbs");

    private final String command;

    Commands(String command) {
        this.command = command;
    }

    public String getUrl(String host) {
        return host.concat(command);
    }
}
