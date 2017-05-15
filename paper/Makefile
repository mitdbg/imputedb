MAIN=main
REBUTTAL=rebuttal

pdf: $(MAIN).tex $(MAIN).bcf $(REBUTTAL).tex
	pdflatex main.tex
	biber main.bcf
	pdflatex main.tex
	pdflatex rebuttal.tex

clean:
	rm -vf *.bbl *.bbg *.blg *.aux *.fls *.fdb_latexmk *.log *.out *.toc *.synctex.gz $(MAIN).pdf
