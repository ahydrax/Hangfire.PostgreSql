﻿ALTER TABLE counter DROP COLUMN id;
ALTER TABLE counter ALTER COLUMN key TYPE TEXT;

ALTER TABLE hash DROP COLUMN id;
ALTER TABLE hash ALTER COLUMN key TYPE TEXT;
ALTER TABLE hash ALTER COLUMN field TYPE TEXT;

ALTER TABLE job ALTER COLUMN id TYPE BIGINT;
ALTER TABLE job ALTER COLUMN stateid TYPE BIGINT;
ALTER TABLE job ALTER COLUMN statename TYPE TEXT;

ALTER TABLE state ALTER COLUMN id TYPE BIGINT;
ALTER TABLE state ALTER COLUMN jobid TYPE BIGINT;
ALTER TABLE state ALTER COLUMN name TYPE TEXT;
ALTER TABLE state ALTER COLUMN reason TYPE TEXT;

ALTER TABLE jobparameter ALTER COLUMN id TYPE BIGINT;
ALTER TABLE jobparameter ALTER COLUMN name TYPE TEXT;
ALTER TABLE jobparameter ALTER COLUMN jobid TYPE BIGINT;

ALTER TABLE jobqueue ALTER COLUMN id TYPE BIGINT;
ALTER TABLE jobqueue ALTER COLUMN jobid TYPE BIGINT;
DROP INDEX ix_hangfire_jobqueue_jobidandqueue;
DROP INDEX ix_hangfire_jobqueue_queueandfetchedat;
CREATE INDEX ix_hangfire_jobqueue_queue_fetchedat ON jobqueue (queue, fetchedat);

ALTER TABLE lock ALTER COLUMN resource TYPE TEXT;

ALTER TABLE list ALTER COLUMN id TYPE BIGINT;
ALTER TABLE list ALTER COLUMN key TYPE TEXT;
CREATE INDEX ix_hangfire_list_key ON list (key);

ALTER TABLE set DROP COLUMN id;
ALTER TABLE set ALTER COLUMN key TYPE TEXT;
ALTER TABLE set DROP CONSTRAINT set_key_value_key;
ALTER TABLE set ADD PRIMARY KEY (key, value);
