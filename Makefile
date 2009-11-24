VERSION = $(shell ./pg_staging.py --version |cut -d' ' -f2)

# debian setting
DESTDIR    =
confdir    = $(DESTDIR)/etc/pg_staging
libdir     = $(DESTDIR)/usr/share/python-support/pg_staging
pg_staging = pg_staging.py
conf       = pg_staging.ini replication.ini
libs       = $(wildcard pgstaging/*.py)

SRC      = .
BUILDDIR = /tmp/pg_staging
ORIG     = pgstaging.orig
DEBIAN   = debian
PACKAGE  = pgstaging{,-client}
SOURCE   = pgstaging


unsign-deb: prepare
	cd $(BUILDDIR)/$(SOURCE) && debuild -us -uc
	cp $(BUILDDIR)/$(PACKAGE)_* ..
	cp $(BUILDDIR)/$(SOURCE)_* ..

deb: prepare
	cd $(BUILDDIR)/$(SOURCE) && debuild
	cp $(BUILDDIR)/$(PACKAGE)_* ..
	cp $(BUILDDIR)/$(SOURCE)_* ..

prepare: clean
	-test -d $(BUILDDIR) && rm -rf $(BUILDDIR)
	mkdir -p $(BUILDDIR)/$(SOURCE)
	rsync -Ca --exclude $(DEBIAN) $(SRC)/* $(BUILDDIR)/$(SOURCE)
	rsync -Ca $(BUILDDIR)/$(SOURCE)/ $(BUILDDIR)/$(ORIG)/
	rsync -Ca $(DEBIAN) $(BUILDDIR)/$(SOURCE)

install: doc
	install -m 755 -d $(confdir)
	install -m 755 $(pg_staging) $(DESTDIR)/usr/bin/pg_staging
	install -m 755 -d $(libdir)/pgstaging

	cp -a $(libs) $(libdir)/pgstaging
	cp -a $(conf) $(confdir)

doc: README.html TODO.html man

man: pg_staging.1 pg_staging.1.html pg_staging.5 pg_staging.5.html

README.html: README.rst
	rst2html $< > $@

TODO.html: TODO
	asciidoc -a toc TODO

pg_staging.1.html: pg_staging.1.txt
	asciidoc -a toc pg_staging.1.txt

pg_staging.1: pg_staging.1.txt
	asciidoc -b docbook -d manpage pg_staging.1.txt
	xmlto man pg_staging.1.xml

pg_staging.5.html: pg_staging.5.txt
	asciidoc -a toc pg_staging.5.txt

pg_staging.5: pg_staging.5.txt
	asciidoc -b docbook -d manpage pg_staging.5.txt
	xmlto man pg_staging.5.xml

clean:
	rm -f *.xml *.html