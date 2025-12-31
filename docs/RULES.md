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

## Persistence Rules (MANDATORY)

- Every derived action MUST be persisted in a state
- Telegram messages are UI only, never a data store
- report.actions MUST always contain the final merged action list
- Learning feedback MUST be persisted before adapter stops

## History Rules

- Only history adapters with getHistory support are allowed
- mysql.0 is NOT a history adapter
- Valid adapters: sql.*, influxdb.*

## Telegram Rules

- Telegram send does NOT replace state updates
- Every Telegram action MUST have a corresponding stored action
- Telegram approval MUST update action status
