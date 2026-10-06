//! Compact order batches (action encoding 1).
//!
//! ```text
//! u32 BE    grouping: 0 na, 1 normalTpsl, 2 positionTpsl, n ≥ 3 custom (n - 3)
//! u8        builder tag: 0 none, 2 followed by a 20-byte address and u64 BE fee
//! orders, until the end of the body:
//!   u32 BE  asset
//!   u64 BE  price × 10⁸
//!   u64 BE  size × 10⁸
//!   u8      flags: bit 0 buy, bit 1 reduce-only, bits 2–5 order type, bit 6 cloid
//!   u64 BE  trigger price × 10⁸, for trigger order types (4–7)
//!   [16]    client order ID, when bit 6 is set
//! ```

use super::Cursor;
use crate::types::{Address, Cloid};
use serde::{Serialize, Serializer};
use std::fmt;
use thiserror::Error;

/// A malformed compact order batch.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum OrderError {
    /// The body ended inside a field.
    #[error("truncated compact order batch")]
    Truncated,
    /// The builder tag is neither 0 nor 2.
    #[error("invalid builder tag {0}")]
    InvalidBuilder(u8),
    /// The order type code is not one of 1–9.
    #[error("invalid order type {0}")]
    InvalidOrderType(u8),
    /// A decimal does not fit in a signed 64-bit integer.
    #[error("decimal out of range")]
    DecimalOutOfRange,
}

/// A batch of orders submitted together.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBatch {
    /// Take-profit/stop-loss grouping of the batch.
    pub grouping: Grouping,
    /// Builder attribution, if any.
    pub builder: Option<Builder>,
    /// Orders in submission order.
    pub orders: Vec<Order>,
}

/// Take-profit/stop-loss grouping, serialized as in the exchange API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grouping {
    /// No grouping (`"na"`).
    Na,
    /// Orders grouped with their TP/SL (`"normalTpsl"`).
    NormalTpsl,
    /// TP/SL attached to the position (`"positionTpsl"`).
    PositionTpsl,
    /// A protocol-defined custom grouping, serialized as `{"custom": n}`.
    Custom(u32),
}

/// A builder and the fee it charges, in raw wire units.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Builder {
    /// Builder address.
    pub address: Address,
    /// Fee in the protocol's raw units.
    pub fee: u64,
}

/// One order request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    /// Asset index.
    pub asset: u32,
    /// Whether the order buys.
    pub is_buy: bool,
    /// Limit price.
    pub price: Decimal8,
    /// Size.
    pub size: Decimal8,
    /// Whether the order may only reduce a position.
    pub reduce_only: bool,
    /// Limit or trigger behavior.
    pub order_type: OrderType,
    /// Client order ID, if any.
    pub cloid: Option<Cloid>,
}

/// Limit or trigger behavior of an order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderType {
    /// A limit order.
    Limit {
        /// Time in force.
        tif: TimeInForce,
    },
    /// A conditional order.
    #[serde(rename_all = "camelCase")]
    Trigger {
        /// Whether the order executes as market once triggered.
        is_market: bool,
        /// Trigger price.
        trigger_px: Decimal8,
        /// Take profit or stop loss.
        tpsl: TpSl,
    },
}

/// Time in force of a limit order, named as in the exchange API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TimeInForce {
    /// Add liquidity only.
    Alo,
    /// Good till cancelled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Market order placed from the frontend.
    FrontendMarket,
    /// Liquidation market order.
    LiquidationMarket,
}

/// Trigger direction of a conditional order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TpSl {
    /// Take profit.
    Tp,
    /// Stop loss.
    Sl,
}

/// An exact decimal with eight fractional digits, displayed without
/// trailing zeros (for example `"123.45"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decimal8(u64);

impl Decimal8 {
    /// Raw units per whole unit.
    pub const SCALE: u64 = 100_000_000;

    /// Wraps a raw value in units of 10⁻⁸.
    pub fn from_raw(raw: u64) -> Result<Self, OrderError> {
        if raw < i64::MAX.cast_unsigned() {
            Ok(Self(raw))
        } else {
            Err(OrderError::DecimalOutOfRange)
        }
    }

    /// Returns the raw value in units of 10⁻⁸.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Decimal8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let whole = self.0 / Self::SCALE;
        let mut fraction = self.0 % Self::SCALE;
        if fraction == 0 {
            return write!(f, "{whole}");
        }
        let mut width = 8;
        while fraction.is_multiple_of(10) {
            fraction /= 10;
            width -= 1;
        }
        write!(f, "{whole}.{fraction:0width$}")
    }
}

impl Serialize for Decimal8 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl Serialize for Grouping {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Na => serializer.serialize_str("na"),
            Self::NormalTpsl => serializer.serialize_str("normalTpsl"),
            Self::PositionTpsl => serializer.serialize_str("positionTpsl"),
            Self::Custom(value) => {
                serializer.serialize_newtype_variant("Grouping", 3, "custom", value)
            }
        }
    }
}

impl OrderBatch {
    /// Decodes a compact order batch body.
    pub fn decode(body: &[u8]) -> Result<Self, OrderError> {
        let mut cursor = Cursor::new(body);
        let grouping = match cursor.u32().ok_or(OrderError::Truncated)? {
            0 => Grouping::Na,
            1 => Grouping::NormalTpsl,
            2 => Grouping::PositionTpsl,
            n => Grouping::Custom(n - 3),
        };
        let builder = match cursor.byte().ok_or(OrderError::Truncated)? {
            0 => None,
            2 => Some(Builder {
                address: Address(cursor.array().ok_or(OrderError::Truncated)?),
                fee: cursor.u64().ok_or(OrderError::Truncated)?,
            }),
            tag => return Err(OrderError::InvalidBuilder(tag)),
        };
        let mut orders = Vec::new();
        while !cursor.is_empty() {
            orders.push(decode_order(&mut cursor)?);
        }
        Ok(Self {
            grouping,
            builder,
            orders,
        })
    }
}

fn decode_order(cursor: &mut Cursor<'_>) -> Result<Order, OrderError> {
    let asset = cursor.u32().ok_or(OrderError::Truncated)?;
    let price = decimal(cursor)?;
    let size = decimal(cursor)?;
    let flags = cursor.byte().ok_or(OrderError::Truncated)?;
    let code = (flags >> 2) & 0x0f;
    let limit = |tif| OrderType::Limit { tif };
    let order_type = match code {
        1 => limit(TimeInForce::Alo),
        2 => limit(TimeInForce::Gtc),
        3 => limit(TimeInForce::Ioc),
        8 => limit(TimeInForce::FrontendMarket),
        9 => limit(TimeInForce::LiquidationMarket),
        4..=7 => OrderType::Trigger {
            trigger_px: decimal(cursor)?,
            is_market: code == 4 || code == 6,
            tpsl: if code <= 5 { TpSl::Tp } else { TpSl::Sl },
        },
        _ => return Err(OrderError::InvalidOrderType(code)),
    };
    let cloid = if flags & 0x40 == 0 {
        None
    } else {
        Some(Cloid(cursor.array().ok_or(OrderError::Truncated)?))
    };
    Ok(Order {
        asset,
        is_buy: flags & 0x01 != 0,
        price,
        size,
        reduce_only: flags & 0x02 != 0,
        order_type,
        cloid,
    })
}

fn decimal(cursor: &mut Cursor<'_>) -> Result<Decimal8, OrderError> {
    Decimal8::from_raw(cursor.u64().ok_or(OrderError::Truncated)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(flags: u8) -> Vec<u8> {
        let mut bytes = 10_001_u32.to_be_bytes().to_vec();
        bytes.extend(12_345_678_901_u64.to_be_bytes());
        bytes.extend(500_000_u64.to_be_bytes());
        bytes.push(flags);
        if (4..=7).contains(&((flags >> 2) & 0x0f)) {
            bytes.extend(12_000_000_000_u64.to_be_bytes());
        }
        if flags & 0x40 != 0 {
            bytes.extend(0_u8..16);
        }
        bytes
    }

    #[test]
    fn formats_exact_decimals() {
        for (raw, text) in [
            (0, "0"),
            (1, "0.00000001"),
            (50_000_000, "0.5"),
            (100_000_000, "1"),
            (12_345_678_900, "123.456789"),
        ] {
            assert_eq!(Decimal8::from_raw(raw).unwrap().to_string(), text);
        }
        assert!(Decimal8::from_raw(u64::MAX).is_err());
    }

    #[test]
    fn decodes_a_builder_batch_with_limit_and_trigger_orders() {
        let mut body = vec![0, 0, 0, 10, 2];
        body.extend([0xab; 20]);
        body.extend(7_u64.to_be_bytes());
        body.extend(order(0x4b)); // buy, reduce-only, Gtc, cloid
        body.extend(order(0x10)); // sell, trigger take-profit market
        let batch = OrderBatch::decode(&body).unwrap();
        assert_eq!(batch.grouping, Grouping::Custom(7));
        assert_eq!(
            batch.builder,
            Some(Builder {
                address: Address([0xab; 20]),
                fee: 7
            })
        );
        let [limit, trigger] = batch.orders.as_slice() else {
            panic!("expected two orders");
        };
        assert!(limit.is_buy && limit.reduce_only);
        assert_eq!(
            limit.order_type,
            OrderType::Limit {
                tif: TimeInForce::Gtc
            }
        );
        assert_eq!(limit.cloid.unwrap().0[15], 15);
        assert!(!trigger.is_buy);
        assert_eq!(
            serde_json::to_value(trigger).unwrap(),
            serde_json::json!({
                "asset": 10001,
                "isBuy": false,
                "price": "123.45678901",
                "size": "0.005",
                "reduceOnly": false,
                "orderType": {"trigger": {"isMarket": true, "triggerPx": "120", "tpsl": "tp"}},
                "cloid": null
            })
        );
        assert_eq!(
            serde_json::to_value(batch.grouping).unwrap(),
            serde_json::json!({"custom": 7})
        );
    }

    #[test]
    fn only_order_types_one_to_nine_are_valid() {
        for flags in 0..=u8::MAX {
            let mut body = vec![0; 5];
            body.extend(order(flags));
            let code = (flags >> 2) & 0x0f;
            assert_eq!(
                OrderBatch::decode(&body).is_ok(),
                (1..=9).contains(&code),
                "flags {flags:#x}"
            );
        }
    }

    #[test]
    fn rejects_partial_orders() {
        let mut body = vec![0; 5];
        body.extend(order(0x53));
        assert!(OrderBatch::decode(&body).is_ok());
        for end in 0..body.len() {
            if end == 5 {
                continue; // a header alone is an empty batch
            }
            assert!(OrderBatch::decode(&body[..end]).is_err(), "prefix {end}");
        }
        assert_eq!(
            OrderBatch::decode(&[0, 0, 0, 0, 1]),
            Err(OrderError::InvalidBuilder(1))
        );
    }
}
