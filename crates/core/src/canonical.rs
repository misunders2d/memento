//! Python json.dumps(sort_keys=True, ensure_ascii=False, allow_nan=False), default spaces.
use crate::{Error, Result};
use serde_json::{Number, Value};
fn number(number: &Number) -> Result<String> {
    let spelling = number.to_string();
    if !spelling.contains(['.', 'e', 'E']) {
        return Ok(spelling);
    }
    let value = spelling
        .parse::<f64>()
        .map_err(|_| Error::Validation("JSON numbers must be finite"))?;
    if !value.is_finite() {
        return Err(Error::Validation("JSON numbers must be finite"));
    }
    if value == 0.0 {
        return Ok(if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .into());
    }
    let shortest = Number::from_f64(value).unwrap().to_string();
    let (sign, unsigned) = shortest
        .strip_prefix('-')
        .map(|s| ("-", s))
        .unwrap_or(("", shortest.as_str()));
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map(|(m, e)| (m, e.parse::<i32>().unwrap()))
        .unwrap_or((unsigned, 0));
    let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits = mantissa.replace('.', "");
    let mut power = point - 1 + exponent;
    while digits.starts_with('0') && digits.len() > 1 {
        digits.remove(0);
        power -= 1;
    }
    while digits.ends_with('0') && digits.len() > 1 {
        digits.pop();
    }
    if !(-4..16).contains(&power) {
        let rest = if digits.len() > 1 {
            format!(".{}", &digits[1..])
        } else {
            String::new()
        };
        return Ok(format!(
            "{sign}{}{rest}e{}{:02}",
            &digits[..1],
            if power < 0 { "-" } else { "+" },
            power.abs()
        ));
    }
    let position = power + 1;
    let result = if position <= 0 {
        format!("0.{}{}", "0".repeat((-position) as usize), digits)
    } else if position as usize >= digits.len() {
        format!(
            "{}{}.0",
            digits,
            "0".repeat(position as usize - digits.len())
        )
    } else {
        format!(
            "{}.{}",
            &digits[..position as usize],
            &digits[position as usize..]
        )
    };
    Ok(format!("{sign}{result}"))
}
pub fn canonical_json(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "null".into(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => number(v)?,
        Value::String(s) => serde_json::to_string(s)?,
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        ),
        Value::Object(values) => {
            let mut pairs = values.iter().collect::<Vec<_>>();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            format!(
                "{{{}}}",
                pairs
                    .into_iter()
                    .map(|(key, value)| Ok(format!(
                        "{}: {}",
                        serde_json::to_string(key)?,
                        canonical_json(value)?
                    )))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )
        }
    })
}
