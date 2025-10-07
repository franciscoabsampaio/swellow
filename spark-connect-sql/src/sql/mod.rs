mod literal;
mod builder;

pub use literal::ToLiteral;
pub(crate) use builder::SqlQueryBuilder;

#[macro_export]
macro_rules! sql {
    ($session:expr, $sql:expr $(, $param:expr)*) => {{
        let params: Vec<LiteralType> = vec![$($param.to_literal()),*];
        $session.sql($sql, params)
    }};
}