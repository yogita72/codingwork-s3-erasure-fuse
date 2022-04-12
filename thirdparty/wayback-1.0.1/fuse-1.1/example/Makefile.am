## Process this file with automake to produce Makefile.in

noinst_PROGRAMS = fusexmp null hello
bin_PROGRAMS = wayback

fusexmp_SOURCES = fusexmp.c
null_SOURCES = null.c
hello_SOURCES = hello.c
wayback_SOURCES = wayback.c waybackfsvops.c

LDADD = ../lib/libfuse.a -lpthread
