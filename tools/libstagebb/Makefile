# Author: Loic Pottier <lpottier@isi.edu>
# Created: 02/04/2020

RM            := rm -f
SUBDIRS       := src
TARGETS       := all clean distclean

$(TARGETS): $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: $(TARGETS) $(SUBDIRS)
