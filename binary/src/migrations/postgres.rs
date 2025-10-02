use phf;

pub static _OID_QUERIES: phf::Map<&'static str, &'static str> = phf::phf_map! {
    // Tables / Indexes / Views / Sequences / Partitions
    "TABLE" => "SELECT $1::regclass::oid::int",
    "INDEX" => "SELECT $1::regclass::oid::int",
    "VIEW" => "SELECT $1::regclass::oid::int",
    "MATERIALIZEDVIEW" => "SELECT $1::regclass::oid::int",
    "SEQUENCE" => "SELECT $1::regclass::oid::int",

    // Types / Functions / Operators
    "TYPE" => "SELECT $1::regtype::oid::int",
    // "function" => "SELECT $1::regproc::oid",
    // "operator" => "SELECT $1::regoperator::oid",
    // "operator_class" => "SELECT $1::regopclass::oid",
    // "operator_family" => "SELECT $1::regopfamily::oid",
    // "collation" => "SELECT $1::regcollation::oid",

    // Schemas / Roles
    "SCHEMA" => "SELECT $1::regnamespace::oid::int",
    "ROLE" => "SELECT $1::regrole::oid::int",
    "USER" => "SELECT $1::regrole::oid::int",  // Users are roles with LOGIN

    // Catalog-only objects (no reg* casts)
    "DATABASE" => "SELECT oid::int FROM pg_database WHERE datname = $1",
    // "tablespace" => "SELECT oid FROM pg_tablespace WHERE spcname = $1",
    // "extension" => "SELECT oid FROM pg_extension WHERE extname = $1",
    // "language" => "SELECT oid FROM pg_language WHERE lanname = $1",
};
