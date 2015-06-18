-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

/*
 * Due necessity to push data from table across process via shared memory
 * where pointers are disallowed, all fields are marked as NOT NULL.
 * It strongly reduce code neccessary to transport data from reader to
 * central scheduler process.
 */
CREATE TABLE job_schedule(
  id serial PRIMARY KEY,
  suspended boolean NOT NULL DEFAULT false,          /* true when job is not active - the cmd is not evaluated */
  max_workers int NOT NULL DEFAULT 10,               /* max numbers of workers related to this job */
  run_workers int NOT NULL DEFAULT 1,                /* number of workers started for specific job, can be higher than max_workers */
  idle_timeout interval NOT NULL DEFAULT '0sec',     /* close idle worker when idle time is longer than */
  life_time interval NOT NULL DEFAULT '0sec',        /* close worker when life is longer than */
  job_start timestamp NOT NULL DEFAULT 'infinity',   /* when job should be started */
  job_repeat_after interval NOT NULL DEFAULT '0sec', /* interval between two runs */
  job_start_timeout interval NOT NULL DEFAULT '30sec',   /* max time from job start to worker start, after this time worker is failed */
  job_timeout interval NOT NULL DEFAULT '-1sec',     /* timeout for execution of SQL statement - -1 default, 0 newer */
  job_user name NOT NULL DEFAULT current_user,       /* used role for execution */
  job_name name NOT NULL DEFAULT '',
  job_cmd text NOT NULL,                             /* executed SQL statement */
  listen_channel name NOT NULL DEFAULT '',           /* when job is emmited by NOTIFY instead time */
  job_start_delay interval NOT NULL DEFAULT '0sec'   /* delay between receiving NOTIFY event and job start, all notifications are merged */
);

-- nonempty job_name should be unique
CREATE UNIQUE INDEX ON job_schedule((CASE WHEN job_name = '' THEN NULL ELSE job_name END));

CREATE FUNCTION scheduler_change_configuration(d job_schedule, op "char")
RETURNS void AS 'MODULE_PATHNAME','jbsch_signal_configuration_change' LANGUAGE C;

CREATE OR REPLACE FUNCTION is_power_user()
RETURNS boolean AS $$
BEGIN
  IF EXISTS(SELECT *
               FROM pg_roles r JOIN pg_database d ON r.oid = d.datdba
              WHERE d.rolname = CURRENT_USER)
  THEN
    RETURN true; -- database owner
  ELSIF EXISTS(SELECT *
                  FROM pg_roles
                 WHERE rolname = CURRENT_USER AND rolsuper)
  THEN
    RETURN true; -- superuser
  END IF;
  RETURN false;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION job_schedule_constraints_insert_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.job_user <> CURRENT_USER THEN
    IF NOT EXISTS(SELECT * FROM pg_roles WHERE rolname = NEW.job_user) THEN
      RAISE EXCEPTION 'Role % doesn''t exists', NEW.job_user;
    END IF;

    /* must be power user */
    IF NOT is_power_user() THEN
      RAISE EXCEPTION 'Role % doesn''t rights to create job for other users on current database', NEW.job_user;
    END IF;
  END IF;
     
  PERFORM scheduler_change_configuration(NEW, 'i');

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_schedule_constraints_insert_trigger AFTER INSERT ON job_schedule
  FOR EACH ROW EXECUTE PROCEDURE job_schedule_constraints_insert_trigger_func();


CREATE OR REPLACE FUNCTION job_schedule_constraints_delete_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.job_user <> CURRENT_USER AND NOT is_power_user() THEN
    RAISE EXCEPTION 'Role % doesn''t rights to drop job for other users on current database', OLD.job_user;
  END IF;
     
  PERFORM scheduler_change_configuration(OLD, 'd');

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_schedule_constraints_delete_trigger AFTER DELETE ON job_schedule
  FOR EACH ROW EXECUTE PROCEDURE job_schedule_constraints_delete_trigger_func();


CREATE OR REPLACE FUNCTION job_schedule_constraints_update_trigger_func()
RETURNS TRIGGER AS $$
DECLARE _is_power_user boolean;
BEGIN
  -- are necessary some checking?
  IF NEW.job_user <> CURRENT_USER OR NEW.job_user <> OLD.job_user THEN
    _is_power_user := is_power_user();

    IF NEW.job_user <> CURRENT_USER THEN
      IF NOT EXISTS(SELECT * FROM pg_roles WHERE rolname = NEW.job_user) THEN
        RAISE EXCEPTION 'Role % doesn''t exists', NEW.job_user;
      END IF;

      /* must be power user */
      IF NOT _is_power_user() THEN
        RAISE EXCEPTION 'Role % doesn''t rights to create job for other users on current database', NEW.job_user;
      END IF;
    END IF;

    IF OLD.job_user <> CURRENT_USER AND NOT _is_power_user() THEN
      RAISE EXCEPTION 'Role % doesn''t rights to update job for other users on current database', OLD.job_user;
    END IF;
  END IF;

  PERFORM scheduler_change_configuration(OLD, 'd');
  PERFORM scheduler_change_configuration(NEW, 'i');

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_schedule_constraints_update_trigger AFTER UPDATE ON job_schedule
  FOR EACH ROW EXECUTE PROCEDURE job_schedule_constraints_update_trigger_func();

/*
 * Disallow truncate
 */
CREATE OR REPLACE FUNCTION job_schedule_disallow_truncate_function()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'TRUNCATE statement is not allowed';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_schedule_constraints_truncate_trigger BEFORE TRUNCATE ON job_schedule
  FOR EACH STATEMENT EXECUTE PROCEDURE job_schedule_disallow_truncate_function();

/*
 * Aux functions for little bit more user friendly job registration
 *
 */
CREATE OR REPLACE FUNCTION register_job_at(job_start timestamp,
                                           job_cmd text, job_user name = current_user, job_name name = '',
                                           max_workers int = 10, run_workers int = 1,
                                           job_start_timeout interval = '10sec', job_timeout interval = '-1sec')
RETURNS int AS $$
BEGIN
  INSERT INTO job_schedule(job_start, job_cmd, job_user, job_name,
                           max_workers, run_workers, job_start_timeout, job_timeout)
    VALUES(job_start, job_cmd, job_user, job_name,
           max_workers, run_workers, job_start_timeout, job_timeout);

  RETURN lastval();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION register_periodic_job(job_start timestamp, job_repeat_after interval,
                                                job_cmd text, job_user name = current_user, job_name name = '',
                                                idle_timeout interval = '0sec', life_time interval = '0sec',
                                                max_workers int = 10, run_workers int = 1,
                                                job_start_timeout interval = '10sec', job_timeout interval = '-1sec')
RETURNS int AS $$
BEGIN
  INSERT INTO job_schedule(job_start, job_repeat_after, job_cmd, job_user, job_name,
                           idle_timeout, life_time, max_workers, run_workers,
                           job_start_timeout, job_timeout)
    VALUES(job_start, job_repeat_after, job_cmd, job_user, job_name,
           idle_timeout, life_time, max_workers, run_workers,
           job_start_timeout, job_timeout);

  RETURN lastval();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION register_listener_job(listen_channel name,
                                                job_cmd text, job_user name = current_user, job_name name = '',
                                                idle_timeout interval = '0sec', life_time interval = '0sec',
                                                max_workers int = 10, run_workers int = 1,
                                                job_start_timeout interval = '10sec', job_timeout interval = '-1sec', job_start_delay interval = '0sec')
RETURNS int AS $$
BEGIN
  IF channel = '' THEN
    RAISE EXCEPTION 'channel is not defined';
  END IF;

  INSERT INTO job_schedule(listen_channel, job_cmd, job_user, job_name,
                           idle_timeout, life_time, max_workers, run_workers,
                           job_start_timeout, job_timeout, job_start_delay)
    VALUES(listen_channel, job_cmd, job_user, job_name,
           idle_timeout, life_time, max_workers, run_workers,
           job_start_timeout, job_timeout, job_start_delay);

  RETURN lastval();
END;
$$ LANGUAGE plpgsql;

/*
 * Enable scheduled jobs per database via security label setting
 *
 */
CREATE OR REPLACE FUNCTION enable_scheduler()
RETURNS void AS $$
BEGIN
  EXECUTE format(e'SECURITY LABEL FOR jobscheduler ON DATABASE %s IS \'active\'', current_database());
END;
$$ LANGUAGE plpgsql;

/*
 * Disable scheduled jobs per database via security label setting
 *
 */
CREATE OR REPLACE FUNCTION disable_scheduler()
RETURNS void AS $$
BEGIN
  EXECUTE format(e'SECURITY LABEL FOR jobscheduler ON DATABASE %s IS \'nonactive\'', current_database());
END;
$$ LANGUAGE plpgsql;

/*
 * Enable scheduler for this database by default.
 *
 */
DO $$
BEGIN
  PERFORM enable_scheduler();
END;
$$;
