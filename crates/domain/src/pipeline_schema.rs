//! The existing native-runner JSON schemas, validated natively and strictly.
use serde_json::Value;
use std::sync::LazyLock;

pub static EXTRACTION: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("pipeline_assets/extraction-schema.json"))
        .expect("frozen extraction schema")
});
pub static CONSOLIDATION: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("pipeline_assets/consolidation-schema.json"))
        .expect("frozen consolidation schema")
});
pub static VALIDATION_CODES: LazyLock<Vec<String>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("pipeline_assets/validation-codes.json"))
        .expect("frozen validation codes")
});
pub const EXTRACTION_INSTRUCTION: &str = include_str!("pipeline_assets/extraction.txt");
pub const CONSOLIDATION_INSTRUCTION: &str = include_str!("pipeline_assets/consolidation.txt");

/// JSON Schema primitives emitted by the pinned native-runner contracts. A
/// strict integer rejects JSON floating point/string/bool exactly as Pydantic.
pub fn validate(schema: &Value, value: &Value) -> bool {
    check(schema, schema, value, 0)
}
fn check(root: &Value, schema: &Value, value: &Value, depth: usize) -> bool {
    if depth > 128 {
        return false;
    }
    if let Some(reference) = schema["$ref"].as_str() {
        let Some(path) = reference.strip_prefix('#') else {
            return false;
        };
        let Some(target) = root.pointer(path) else {
            return false;
        };
        return check(root, target, value, depth + 1);
    }
    if let Some(alternatives) = schema["anyOf"].as_array() {
        if !alternatives
            .iter()
            .any(|schema| check(root, schema, value, depth + 1))
        {
            return false;
        }
    }
    if let Some(alternatives) = schema["oneOf"].as_array() {
        if alternatives
            .iter()
            .filter(|schema| check(root, schema, value, depth + 1))
            .count()
            != 1
        {
            return false;
        }
    }
    if let Some(alternatives) = schema["allOf"].as_array() {
        if !alternatives
            .iter()
            .all(|schema| check(root, schema, value, depth + 1))
        {
            return false;
        }
    }
    if let Some(values) = schema["enum"].as_array() {
        if !values.contains(value) {
            return false;
        }
    }
    if let Some(constant) = schema.get("const") {
        if constant != value {
            return false;
        }
    }
    if let Some(kind) = schema["type"].as_str() {
        let valid = match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "null" => value.is_null(),
            "boolean" => value.is_boolean(),
            "integer" => value.is_i64() || value.is_u64(),
            "number" => value.as_f64().is_some_and(f64::is_finite),
            _ => false,
        };
        if !valid {
            return false;
        }
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema["required"].as_array() {
            if required
                .iter()
                .any(|key| !key.as_str().is_some_and(|key| object.contains_key(key)))
            {
                return false;
            }
        }
        if let Some(properties) = schema["properties"].as_object() {
            for (key, value) in object {
                if let Some(property) = properties.get(key) {
                    if !check(root, property, value, depth + 1) {
                        return false;
                    }
                } else if schema["additionalProperties"] == false {
                    return false;
                }
            }
        }
    }
    if let Some(array) = value.as_array() {
        if schema["minItems"]
            .as_u64()
            .is_some_and(|minimum| array.len() < minimum as usize)
            || schema["maxItems"]
                .as_u64()
                .is_some_and(|maximum| array.len() > maximum as usize)
        {
            return false;
        }
        if let Some(item) = schema.get("items") {
            if !array
                .iter()
                .all(|value| check(root, item, value, depth + 1))
            {
                return false;
            }
        }
        if schema["uniqueItems"] == true {
            for (index, item) in array.iter().enumerate() {
                if array[..index].contains(item) {
                    return false;
                }
            }
        }
    }
    if let Some(text) = value.as_str() {
        let length = text.chars().count();
        if schema["minLength"]
            .as_u64()
            .is_some_and(|minimum| length < minimum as usize)
            || schema["maxLength"]
                .as_u64()
                .is_some_and(|maximum| length > maximum as usize)
        {
            return false;
        }
        if let Some(pattern) = schema["pattern"].as_str() {
            let Ok(pattern) = regex::Regex::new(pattern) else {
                return false;
            };
            if !pattern.is_match(text) {
                return false;
            }
        }
    }
    if let Some(number) = value.as_f64() {
        if schema["minimum"]
            .as_f64()
            .is_some_and(|bound| number < bound)
            || schema["maximum"]
                .as_f64()
                .is_some_and(|bound| number > bound)
            || schema["exclusiveMinimum"]
                .as_f64()
                .is_some_and(|bound| number <= bound)
            || schema["exclusiveMaximum"]
                .as_f64()
                .is_some_and(|bound| number >= bound)
        {
            return false;
        }
    }
    true
}

#[derive(Debug)]
pub struct WorkError {
    pub error: memento_core::Error,
    pub references: Vec<Value>,
}
pub type WorkResult<T> = Result<T, WorkError>;
impl From<memento_core::Error> for WorkError {
    fn from(error: memento_core::Error) -> Self {
        Self {
            error,
            references: vec![],
        }
    }
}
impl From<rusqlite::Error> for WorkError {
    fn from(error: rusqlite::Error) -> Self {
        Self {
            error: error.into(),
            references: vec![],
        }
    }
}
impl From<serde_json::Error> for WorkError {
    fn from(error: serde_json::Error) -> Self {
        Self {
            error: error.into(),
            references: vec![],
        }
    }
}
pub fn reject(code: &'static str, references: Vec<Value>) -> WorkError {
    let mut bounded = vec![];
    for reference in references.into_iter().take(3) {
        if let Some(fields) = reference.as_object() {
            let mut selected = serde_json::Map::new();
            for (key, value) in fields {
                if ![
                    "candidate_id",
                    "window_id",
                    "target",
                    "key",
                    "operation",
                    "episode_id",
                    "occurrence",
                    "quote",
                ]
                .contains(&key.as_str())
                {
                    continue;
                }
                let value = if key == "quote" {
                    value
                        .as_str()
                        .map(|value| Value::String(value.chars().take(300).collect()))
                        .unwrap_or(value.clone())
                } else {
                    value.clone()
                };
                selected.insert(key.clone(), value);
            }
            bounded.push(Value::Object(selected));
        }
    }
    WorkError {
        error: memento_core::Error::Validation(code),
        references: bounded,
    }
}
