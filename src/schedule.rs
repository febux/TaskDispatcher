//! Schedule seed computation.
//!
//! Given a spec's schedule fields, compute `next_run` as a UTC instant.
//! This is the *initial* seed written by the API (DESIGN §2.3); the
//! scheduler (Phase 2) re-seeds `next_run` after every successful fire
//! (DESIGN §2.2) using the same cron crate.
//!
//! Timezone handling: the cron expression is interpreted in the spec's tz
//! (`chrono-tz`), then converted to UTC. DST gaps are handled by chrono-tz.
//! This is the DESIGN §3 "store tz per task, compute in UTC" contract.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use cron::Schedule;

use crate::models::SpecType;

/// Compute the next UTC run time for a spec, given `now` as the reference.
///
/// Returns `Ok(None)` when the spec has no future occurrence (e.g. a
/// one-shot whose `run_at` is already in the past). The scheduler decides
/// catch-up behaviour (DESIGN §3); here we only produce the next seed.
pub fn compute_next_run(
    spec_type: SpecType,
    cron_expr: Option<&str>,
    interval_seconds: Option<i64>,
    run_at: Option<DateTime<Utc>>,
    timezone: &str,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    match spec_type {
        SpecType::Once => {
            // The seed is the run_at itself; past one-shots carry no next run.
            Ok(run_at.filter(|t| *t > now))
        }
        SpecType::Interval => {
            let secs = interval_seconds.context("interval_seconds required for interval spec")?;
            if secs <= 0 {
                bail!("interval_seconds must be positive");
            }
            Ok(Some(now + Duration::seconds(secs)))
        }
        SpecType::Cron => {
            let expr = cron_expr.context("cron_expr required for cron spec")?;
            let tz: Tz = timezone
                .parse()
                .with_context(|| format!("invalid timezone {timezone:?}"))?;
            let sched: Schedule = expr
                .parse()
                .with_context(|| format!("invalid cron expression {expr:?}"))?;
            let now_local = now.with_timezone(&tz);
            let next_local = sched
                .after(&now_local)
                .next()
                .with_context(|| format!("cron expression {expr:?} has no future occurrences"))?;
            Ok(Some(next_local.with_timezone(&Utc)))
        }
    }
}

/// Cron format hint surfaced in error messages and docs. The `cron` crate
/// uses 6-7 fields (sec min hour day-of-month month day-of-week [year]),
/// NOT 5-field unix cron.
pub const CRON_FORMAT_HINT: &str =
    "cron expects 6-7 fields: 'sec min hour day-of-month month day-of-week [year]' \
     (e.g. '0 0 9 * * Mon-Fri' = 09:00 Mon–Fri)";
