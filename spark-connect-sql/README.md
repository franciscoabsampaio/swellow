# Spark Connect SQL

## Querying

Exposes two interfaces:

sql(query, params)

query(query_string).bind(param).bind(param)

whereas sql() returns a lazily-evaluated plan that must be collected,

query() and execute() automatically collect the data.

Behind the scenes, both are doing the same, so there is no performance benefit to either.