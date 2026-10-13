# Governed watcher

Read-only liveness polling and incident analysis, with optional governed Telegram
notifications. See the root [setup guide](../../docs/setup.md),
[configuration reference](../../docs/configuration.md), and
[engineering limitations](../../docs/engineering-decisions.md).

By default, `WATCHER_DRY_RUN=true` and `WATCHER_ALLOWED_ACTIONS` is empty.
The watcher analyzes open runs for stalls, correlated inactivity, failures,
quarantine and reported telemetry degradation. It records local audit decisions.
Telegram is the implemented external action; no shell remediation is dispatched.
Governance budgets and incident keys are in memory and reset on restart.

The end-of-run message formatter applies configured field-name exclusions to
structured data before building a message. `summary` and final-message preview
are excluded by default, including short strings. A separate text helper only
limits length; it is not general secret detection. Arbitrary fields, labels,
paths or messages outside those exclusions may remain sensitive. Dry-run logs
are local data, not inherently safe to publish.

Tests use synthetic incidents and mocked transport. Governance state is in memory, so limits reset when the watcher restarts.
