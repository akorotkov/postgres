# contrib/pg_stat_wait/Makefile

MODULE_big = pg_stat_wait
OBJS = pg_stat_wait.o collector.o

EXTENSION = pg_stat_wait
DATA = pg_stat_wait--1.0.sql
PG_CPPFLAGS = -DPG_STAT_WAIT_TESTS
EXTRA_CLEAN = $(pg_regress_clean_files) ./regression_output ./isolation_output

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_stat_wait
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check: isolationcheck

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-pg_stat_wait:
	$(MAKE) -C $(top_builddir)/contrib/pg_stat_wait

REGRESSCHECKS=descriptions file_trace

regresscheck: all | submake-regress submake-pg_stat_wait
	$(MKDIR_P) regression_output
	$(pg_regress_check) \
	    --temp-install=./tmp_check \
	    --extra-install=contrib/pg_stat_wait \
	    --outputdir=./regression_output \
	    $(REGRESSCHECKS)

ISOLATIONCHECKS=history

isolationcheck: all | submake-isolation submake-pg_stat_wait
	$(MKDIR_P) isolation_output
	$(pg_isolation_regress_check) \
	    --extra-install=contrib/pg_stat_wait \
		--temp-config=./waits.conf \
	    --outputdir=./isolation_output \
	    $(ISOLATIONCHECKS)

