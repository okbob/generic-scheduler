/* src/test/modules/dummy_seclabel/dummy_seclabel--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

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

CREATE UNIQUE INDEX ON job_schedule((CASE WHEN job_name = '' THEN NULL ELSE job_name END));

CREATE FUNCTION scheduler_change_configuration(d job_schedule, op "char")
RETURNS void AS 'MODULE_PATHNAME','jbsch_signal_configuration_change' LANGUAGE C;

CREATE OR REPLACE FUNCTION enable_scheduler()
RETURNS void AS $$
BEGIN
  EXECUTE format(e'SECURITY LABEL FOR jobscheduler ON DATABASE %s IS \'active\'', current_database());
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION disable_scheduler()
RETURNS void AS $$
BEGIN
  EXECUTE format(e'SECURITY LABEL FOR jobscheduler ON DATABASE %s IS \'nonactive\'', current_database());
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION job_schedule_constraints_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
  /*
   * job_user must be known user, same as current user, or any when current user is database owner
   * or super user.
   */
   IF NEW.job_user <> CURRENT_USER THEN
     IF NOT EXISTS(SELECT * FROM pg_roles WHERE rolname = NEW.job_user) THEN
       RAISE EXCEPTION 'Role % doesn''t exists', NEW.job_user;
       IF NOT EXISTS (SELECT * FROM pg_roles WHERE rolname = CURRENT_USER AND rolsuper)
          AND NOT EXISTS(SELECT * FROM pg_roles r JOIN pg_database d ON r.oid = d.datdba AND d.rolname = CURRENT_USER)
       THEN
         RAISE EXCEPTION 'Role % doesn''t rights to set job on current database', NEW.job_user;
       END IF;
     END IF;
   END IF;

  PERFORM scheduler_change_configuration(NEW, (CASE WHEN TG_OP = 'INSERT' THEN 'i' ELSE 'u' END)::"char");

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_schedule_constraints_trigger AFTER INSERT OR UPDATE ON job_schedule
  FOR EACH ROW EXECUTE PROCEDURE job_schedule_constraints_trigger_func();

DO $$
BEGIN
  PERFORM enable_scheduler();
END;
$$;

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

