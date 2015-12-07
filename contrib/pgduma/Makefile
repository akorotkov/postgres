# contrib/pgduma/Makefile

MODULE_big = pgduma
OBJS = pgduma.o

EXTENSION = pgduma
DATA = pgduma--1.0.sql

REGRESS=pgduma

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgduma
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
