MODULE_big   = schedulerx
OBJS = jobcfg.o dbworker.o dbwpool.o scheduler.o simplesqlcmd.o

EXTENSION = schedulerx
DATA = schedulerx--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
