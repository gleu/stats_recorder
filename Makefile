# stats_recorder_spi/Makefile

MODULES = stats_recorder_spi

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
