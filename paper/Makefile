.DEFAULT_TARGET := help

MAIN=main
REBUTTAL=rebuttal

help:
	@echo "make help"
	@echo "    all: compile main.pdf and rebuttal.pdf"
	@echo "    main: compile main.pdf"
	@echo "    rebuttal: compile rebuttal.pdf"
	@echo "    clean: clean auxiliary files for main and rebuttal"
	@echo
	@echo "    *Note*: uses \`latexmk' for compilation management. Please install latexmk"
	@echo "            if you do not have it already."

all: $(MAIN) $(REBUTTAL)

$(MAIN):
	latexmk -pdf $@

$(REBUTTAL):
	latexmk -pdf $@

clean:
	latexmk -quiet -C $(MAIN)
	latexmk -quiet -C $(REBUTTAL)

