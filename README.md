# Redis Solution Starter - ReasonML

This is a starting point for ReasonML solutions to the Redis Challenge.

**Steps to get started**:

- Get a leaderboard URL & API key from Paul
- Ensure your API key is available as `$REDIS_CHALLENGE_API_KEY`
(env var)
- Ensure you have `esy` installed locally
- Clone this repository

Note: To install `esy`, I used `sudo npm install -g esy --unsafe-perm=true`

**Workflow**:

- Run `make download_tester_mac` (or `download_tester_linux`, if you're running linux)
- Run `make test`. You should see a failure message at this point.
- Implement the required feature in `app/Main.hs`, iterate until `make test` passes.
- If you want more verbose output for errors, use `make test_debug` instead of `make test`)
- Once `make test` passes, run `make test_and_report`.
- Bump `current_stage` in your Makefile to go to the next stage!
