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
