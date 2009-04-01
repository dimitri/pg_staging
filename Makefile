doc: README.html pg_staging.1 pg_staging.1.html pg_staging.5 pg_staging.5.html

README.html:
	asciidoc -a toc README

pg_staging.1.html: pg_staging.1.txt
	asciidoc -a toc pg_staging.1.txt

pg_staging.1: pg_staging.1.txt
	asciidoc -a toc -b docbook pg_staging.1.txt
	xmlto man pg_staging.1.xml

pg_staging.5.html: pg_staging.5.txt
	asciidoc -a toc pg_staging.5.txt

pg_staging.5: pg_staging.5.txt
	asciidoc -a toc -b docbook pg_staging.5.txt
	xmlto man pg_staging.5.xml
