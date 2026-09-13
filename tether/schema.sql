-- Tether schema.
--
-- One rule runs through this file: every timestamp is produced by the database,
-- never by a worker. Workers disagree about what time it is; the row that holds
-- the lease is the only arbiter that matters.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_state') THEN
        CREATE TYPE task_state AS ENUM ('queued', 'leased', 'done', 'dead');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS tasks (
    id              bigserial PRIMARY KEY,
    queue           text        NOT NULL,
    kind            text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    state           task_state  NOT NULL DEFAULT 'queued',

    -- Ordering. `boost` is the starvation guard: the reaper raises it for rows
    -- that have waited too long, so the effective priority is a stored integer
    -- the index can sort on rather than an expression over now().
    priority        smallint    NOT NULL DEFAULT 0,
    boost           smallint    NOT NULL DEFAULT 0,

    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5,

    available_at    timestamptz NOT NULL DEFAULT now(),
    leased_until    timestamptz,
    leased_by       text,
    lease_ttl_ms    int         NOT NULL DEFAULT 30000,

    idempotency_key text,
    last_error      text,

    enqueued_at     timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    dead_at         timestamptz,

    CONSTRAINT lease_fields_agree CHECK (
        (state = 'leased' AND leased_until IS NOT NULL AND leased_by IS NOT NULL)
        OR (state <> 'leased' AND leased_until IS NULL AND leased_by IS NULL)
    )
);

-- Producer-side idempotency: enqueueing the same key twice on the same queue
-- returns the original task instead of creating a second one.
CREATE UNIQUE INDEX IF NOT EXISTS tasks_idempotency
    ON tasks (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The lease query's index. Column order matches its ORDER BY exactly, and the
-- partial predicate keeps done and dead rows out of it entirely.
CREATE INDEX IF NOT EXISTS tasks_ready
    ON tasks (queue, ((priority + boost)) DESC, available_at)
    WHERE state = 'queued';

-- The reaper's index.
CREATE INDEX IF NOT EXISTS tasks_expiring
    ON tasks (leased_until)
    WHERE state = 'leased';

-- Counting in-flight work per queue, for the concurrency cap.
CREATE INDEX IF NOT EXISTS tasks_in_flight
    ON tasks (queue)
    WHERE state = 'leased';

-- Consumer-side idempotency ledger. A handler that performs an external effect
-- claims a key here; the claim and the acknowledgement commit in one
-- transaction, so a redelivered task finds the claim and skips the effect.
CREATE TABLE IF NOT EXISTS effects (
    effect_key  text PRIMARY KEY,
    task_id     bigint      NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);

-- Demonstration table. The example handlers write here so the test suite can
-- count how many times an effect actually happened.
CREATE TABLE IF NOT EXISTS charges (
    id          bigserial PRIMARY KEY,
    account     text        NOT NULL,
    amount_cents bigint     NOT NULL,
    task_id     bigint      NOT NULL,
    charged_at  timestamptz NOT NULL DEFAULT now()
);
