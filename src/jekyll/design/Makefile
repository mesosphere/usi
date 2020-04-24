PANDOC=/usr/local/bin/pandoc --template /usr/local/etc/pandoc/templates/pandoc-goodies/templates/html5/github/GitHub.html5

default: all
%.svg: %.pu
	plantuml -tsvg $*.pu

index.html: index.md
	cat index.md | $(PANDOC) -f markdown  --toc --toc-depth=2 > $@.work
	mv $@.work $@
all: index.html $(patsubst %.pu,%.svg,$(wildcard *.pu))
	echo Done

