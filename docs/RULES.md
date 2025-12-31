# AI Autopilot – Core Rules

## Absolute Rules

- control.run is an impulse, never a state
- control.run must always reset to false immediately (ack=true)
- Admin schema is a contract – never remove fields
- New features must be additive only
- GPT must never be called without live context
- Debug logging must be optional and switchable
- runAnalysis() MUST NEVER exit early
- runAnalysis() MUST have exactly one exit point using try/finally
- this.running MUST be reset in finally under ALL circumstances
- report.last MUST ALWAYS be written (even if no actions)
- report.actions MUST ALWAYS be written (empty array is valid)
- Telegram notifications MUST be sent even if no actions exist

## Failure Conditions (Bugs)

- Adapter is green but idle → BUG
- control.run remains true → BUG
- this.running remains true → BUG
- Admin tabs missing → BUG
- report.last not updated after trigger → BUG
- Telegram not sent after analysis → BUG
