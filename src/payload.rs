//! Minimal payload templating (Phase 4, DESIGN §7.5).
//!
//! The only supported substitution is `{{ scheduled_time }}`, replaced with
//! the UTC scheduled fire time formatted as RFC3339. It is replaced only when
//! a JSON string value is *exactly* that token (after trimming). Nested objects
//! and arrays are walked recursively.
//!
//! This is intentionally tiny: templates are data, not code (DESIGN §4).

use chrono::{DateTime, Utc};

/// Render a payload template with the scheduled fire time.
pub fn render(payload: &serde_json::Value, scheduled_fire_time: DateTime<Utc>) -> serde_json::Value {
    let time_str = scheduled_fire_time.to_rfc3339();
    render_inner(payload, &time_str)
}

fn render_inner(v: &serde_json::Value, time_str: &str) -> serde_json::Value {
    match v {
        serde_json::Value::String(s) if s.trim() == "{{ scheduled_time }}" => {
            serde_json::Value::String(time_str.to_string())
        }
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, child) in map {
                out.insert(k.clone(), render_inner(child, time_str));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(|child| render_inner(child, time_str)).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn replaces_exact_token() {
        let t = Utc::now();
        let rendered = render(&json!({ "when": "{{ scheduled_time }}" }), t);
        assert_eq!(rendered["when"].as_str().unwrap(), t.to_rfc3339());
    }

    #[test]
    fn leaves_other_strings_and_values_intact() {
        let t = Utc::now();
        let rendered = render(&json!({ "keep": "hello", "count": 42, "flag": true, "arr": ["{{ scheduled_time }}", "stay"] }), t);
        assert_eq!(rendered["keep"], "hello");
        assert_eq!(rendered["count"], 42);
        assert_eq!(rendered["flag"], true);
        assert_eq!(rendered["arr"][0].as_str().unwrap(), t.to_rfc3339());
        assert_eq!(rendered["arr"][1], "stay");
    }
}
