#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/vodka
#
# IDENTIFICATION
#    src/backend/access/vodka/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/vodka
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = vodkautil.o vodkainsert.o vodkaxlog.o vodkaentrypage.o vodkadatapage.o \
	vodkabtree.o vodkascan.o vodkaget.o vodkavacuum.o vodkajsonbproc.o \
	vodkabulk.o vodkafast.o vodkapostinglist.o vodkalogic.o

include $(top_srcdir)/src/backend/common.mk
