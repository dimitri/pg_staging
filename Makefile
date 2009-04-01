doc: README.html pg_staging.1

README.html:
	asciidoc -a toc README

pg_staging.1:
	asciidoc -a toc -b docbook pg_staging.1.txt
	xmlto man pg_staging.1.xml
