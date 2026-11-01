//! Automatic loop due dates: an explicit date or deadline in the cited source,
//! resolved by the model against the source's original local time and checked here.
use crate::util::{format_time, parse_time};
use chrono::{DateTime, Datelike, LocalResult, NaiveDate, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::Value;

/// Why a proposed due date is refused; each pipeline maps it to its fixed code.
#[derive(Debug, PartialEq, Eq)]
pub enum Invalid {
    /// Malformed date, unknown source time/timezone or contradictory `overdue`.
    Format,
    /// The date expression is not part of the cited quote.
    Quote,
    /// Earlier than the source's original time without an explicit overdue statement.
    BeforeSource,
}

/// The source's original time in the processing timezone with its weekday, e.g.
/// `2026-11-01T14:03:00+02:00 Sunday`; `None` when the time or timezone is unknown.
pub fn local_time(occurred_at: Option<&str>, zone: Option<&str>) -> Option<String> {
    let zone: Tz = zone?.parse().ok()?;
    let local = parse_time(occurred_at?).ok()?.with_timezone(&zone);
    Some(format!(
        "{} {}",
        local.to_rfc3339_opts(SecondsFormat::Secs, false),
        local.format("%A")
    ))
}

/// Resolves `due` (`{at, quote, overdue}`) for an open loop cited by `cited`.
/// `at` is a calendar date `YYYY-MM-DD` (a date-only deadline: the end of that
/// local day, 23:59:59 in `zone`) or an aware ISO timestamp for a stated time of
/// day. Returns the UTC-normalized form stored in `loop_events.due_at`.
pub fn resolve(
    due: &Value,
    cited: &str,
    source_at: Option<&str>,
    zone: Option<&str>,
) -> Result<String, Invalid> {
    let expression = due["quote"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or(Invalid::Format)?;
    if !cited.contains(expression) {
        return Err(Invalid::Quote);
    }
    let source = parse_time(source_at.ok_or(Invalid::Format)?).map_err(|_| Invalid::Format)?;
    let at = due["at"].as_str().ok_or(Invalid::Format)?;
    let instant = match NaiveDate::parse_from_str(at, "%Y-%m-%d") {
        Ok(date) => end_of_day(date, zone.and_then(|zone| zone.parse::<Tz>().ok()).ok_or(Invalid::Format)?)?,
        Err(_) => parse_time(at).map_err(|_| Invalid::Format)?,
    };
    // `overdue` states the deadline had already passed when the source was written.
    match (due["overdue"].as_bool(), instant < source) {
        (Some(false), true) => Err(Invalid::BeforeSource),
        (Some(overdue), past) if overdue == past => Ok(format_time(instant)),
        _ => Err(Invalid::Format),
    }
}

/// 23:59:59 local time on `date`. A repeated local time takes the later instant;
/// a date whose end does not exist locally is refused.
fn end_of_day(date: NaiveDate, zone: Tz) -> Result<DateTime<Utc>, Invalid> {
    if !(1..=9999).contains(&date.year()) {
        return Err(Invalid::Format);
    }
    let local = date.and_hms_opt(23, 59, 59).ok_or(Invalid::Format)?;
    match zone.from_local_datetime(&local) {
        LocalResult::Single(at) | LocalResult::Ambiguous(_, at) => Ok(at.with_timezone(&Utc)),
        LocalResult::None => Err(Invalid::Format),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn due(at: &str, quote: &str, overdue: bool) -> Value {
        json!({"at":at,"quote":quote,"overdue":overdue})
    }
    const SOURCE: &str = "we'll be uploading assets there (tomorrow)";

    #[test]
    fn date_only_deadline_is_end_of_local_day_across_zones_and_dst() {
        // The owner's example: written on 2026-11-01, "tomorrow" is 2026-11-02.
        for (zone, sent, expected) in [
            ("UTC", "2026-11-01T10:00:00Z", "2026-11-02T23:59:59+00:00"),
            ("Europe/Berlin", "2026-11-01T10:00:00Z", "2026-11-02T22:59:59+00:00"),
            // New York leaves DST on 2026-11-01; the next day is EST (-05:00).
            ("America/New_York", "2026-11-01T10:00:00Z", "2026-11-03T04:59:59+00:00"),
            ("Asia/Kolkata", "2026-11-01T10:00:00Z", "2026-11-02T18:29:59+00:00"),
        ] {
            assert_eq!(resolve(&due("2026-11-02", "tomorrow", false), SOURCE, Some(sent), Some(zone)), Ok(expected.into()), "{zone}");
        }
        // Berlin DST: the day clocks go forward (+02:00) and the day they go back (+01:00).
        let berlin = |date: &str| resolve(&due(date, "tomorrow", false), SOURCE, Some("2026-03-01T00:00:00Z"), Some("Europe/Berlin"));
        assert_eq!(berlin("2026-03-28"), Ok("2026-03-28T22:59:59+00:00".into()));
        assert_eq!(berlin("2026-03-29"), Ok("2026-03-29T21:59:59+00:00".into()));
        assert_eq!(berlin("2026-10-24"), Ok("2026-10-24T21:59:59+00:00".into()));
        assert_eq!(berlin("2026-10-25"), Ok("2026-10-25T22:59:59+00:00".into()));
        // A whole local day that never existed (Samoa skipped 2011-12-30).
        let skipped = due("2011-12-30", "tomorrow", false);
        assert_eq!(resolve(&skipped, SOURCE, Some("2011-12-01T00:00:00Z"), Some("Pacific/Apia")), Err(Invalid::Format));
    }

    #[test]
    fn source_local_time_is_rendered_in_the_processing_zone() {
        // Late evening UTC is already the next local day east of UTC and not yet west.
        let at = Some("2026-11-01T23:30:00Z");
        assert_eq!(local_time(at, Some("Europe/Berlin")).as_deref(), Some("2026-11-02T00:30:00+01:00 Monday"));
        assert_eq!(local_time(at, Some("America/New_York")).as_deref(), Some("2026-11-01T18:30:00-05:00 Sunday"));
        assert_eq!(local_time(Some("2026-10-25T00:30:00Z"), Some("Europe/Berlin")).as_deref(), Some("2026-10-25T02:30:00+02:00 Sunday"));
        assert_eq!(local_time(None, Some("UTC")), None);
        assert_eq!(local_time(at, Some("Not/AZone")), None);
    }

    #[test]
    fn timed_deadline_is_aware_iso_and_naive_or_vague_values_are_refused() {
        let sent = Some("2026-11-01T10:00:00Z");
        let timed = due("2026-11-03T15:00:00+01:00", "by 3pm on Nov 3", false);
        assert_eq!(resolve(&timed, "send it by 3pm on Nov 3", sent, Some("Europe/Berlin")), Ok("2026-11-03T14:00:00+00:00".into()));
        for at in ["2026-11-03T15:00:00", "soon", "", "2026-13-01", "Nov 3"] {
            assert_eq!(resolve(&due(at, "Nov 3", false), "on Nov 3", sent, Some("UTC")), Err(Invalid::Format), "{at}");
        }
        // Unknown original time or timezone: no due date.
        assert_eq!(resolve(&due("2026-11-02", "tomorrow", false), SOURCE, None, Some("UTC")), Err(Invalid::Format));
        assert_eq!(resolve(&due("2026-11-02", "tomorrow", false), SOURCE, sent, None), Err(Invalid::Format));
    }

    #[test]
    fn date_expression_must_be_quoted_and_past_dates_need_overdue() {
        let sent = Some("2026-11-01T10:00:00Z");
        // An invented date whose expression is absent from the quote.
        assert_eq!(resolve(&due("2026-11-06", "by Friday", false), SOURCE, sent, Some("UTC")), Err(Invalid::Quote));
        assert_eq!(resolve(&due("2026-11-02", " ", false), SOURCE, sent, Some("UTC")), Err(Invalid::Format));
        assert_eq!(resolve(&due("2026-10-30", "tomorrow", false), SOURCE, sent, Some("UTC")), Err(Invalid::BeforeSource));
        let late = "the report was due on Oct 30 and is overdue";
        assert_eq!(resolve(&due("2026-10-30", "due on Oct 30", true), late, sent, Some("UTC")), Ok("2026-10-30T23:59:59+00:00".into()));
        // Overdue contradicts a future deadline.
        assert_eq!(resolve(&due("2026-11-02", "tomorrow", true), SOURCE, sent, Some("UTC")), Err(Invalid::Format));
        // Today's end is never before a source written that day.
        assert_eq!(resolve(&due("2026-11-01", "today", false), "finish it today", sent, Some("UTC")), Ok("2026-11-01T23:59:59+00:00".into()));
    }
}
