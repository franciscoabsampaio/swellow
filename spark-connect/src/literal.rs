use crate::spark::expression::literal::{LiteralType, Decimal, CalendarInterval, Array, Map, Struct};
use crate::spark::expression::Literal;
use crate::spark::DataType;

#[cfg(feature = "chrono")]
use chrono::{NaiveDate, NaiveDateTime};

/// A trait that allows automatic conversion of Rust primitives and complex types into Spark data types.
/// 
/// # Overview
/// 
/// The `ToLiteral` trait provides a unified interface for converting Rust primitive and complex types
/// into Spark SQL `Literal` values. Implementations of this trait allow seamless and type-safe
/// construction of Spark literals from native Rust values, supporting both primitive types (such as
/// integers, floats, booleans, and strings) and complex types (such as arrays, maps, and structs).
/// 
/// Special cases are handled for optional types and for date/time types when the `chrono`
/// feature is enabled.
///
/// # Examples
///
/// ```rust
/// use spark_connect::ToLiteral;
///
/// let lit = 42i32.to_literal(); // Converts i32 to Literal { literal_type: Some(LiteralType::Integer) }
/// let lit = "hello".to_literal(); // Converts &str to Literal { literal_type: Some(LiteralType::String) }
/// ```
///
/// This trait is intended for use with the [`SparkSession::query()`](crate::SparkSession::query)
/// and [`SparkSession::sql()`](crate::SparkSession::sql) methods
/// to facilitate construction of parameterized queries.
pub trait ToLiteral {
    fn to_literal(self) -> Literal;
}

impl Literal {
    pub fn from_type(lit: LiteralType) -> Self {
        Literal { literal_type: Some(lit) }
    }
}

/// Macro to implement ToLiteral for a type mapping to a LiteralType variant.
macro_rules! impl_to_literal {
    ($ty:ty => $variant:ident) => {
        impl ToLiteral for $ty {
            fn to_literal(self) -> Literal {
                Literal::from_type(LiteralType::$variant(self))
            }
        }
    };
}

// Primitives
impl_to_literal!(i32 => Integer);
impl_to_literal!(i64 => Long);
impl_to_literal!(f32 => Float);
impl_to_literal!(f64 => Double);
impl_to_literal!(bool => Boolean);
impl_to_literal!(String => String);
impl_to_literal!(Vec<u8> => Binary);

// Special cases
impl ToLiteral for i16 {
    fn to_literal(self) -> Literal {
        Literal::from_type(LiteralType::Short(self.into()))
    }
}
impl ToLiteral for &str {
    fn to_literal(self) -> Literal {
        Literal::from_type(LiteralType::String(self.to_string()))
    }
}

// Complex types
impl_to_literal!(Decimal => Decimal);
impl_to_literal!(CalendarInterval => CalendarInterval);
impl_to_literal!(Array => Array);
impl_to_literal!(Map => Map);
impl_to_literal!(Struct => Struct);

// Option<DataType> as Null
impl ToLiteral for Option<DataType> {
    fn to_literal(self) -> Literal {
        Literal::from_type(LiteralType::Null(self.unwrap_or_default()))
    }
}

// Optional LiteralType
impl ToLiteral for Option<LiteralType> {
    fn to_literal(self) -> Literal {
        Literal::from_type(self.unwrap_or(LiteralType::Null(Default::default())))
    }
}

#[cfg(feature = "chrono")]
impl ToLiteral for NaiveDate {
    fn to_literal(self) -> Literal {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days_since_epoch = self.signed_duration_since(epoch).num_days() as i32;
        Literal::from_type(LiteralType::Date(days_since_epoch))
    }
}

#[cfg(feature = "chrono")]
impl ToLiteral for NaiveDateTime {
    fn to_literal(self) -> Literal {
        Literal::from_type(LiteralType::Timestamp(self.and_utc().timestamp_micros()))
    }
}
