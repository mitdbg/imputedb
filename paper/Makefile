MAIN=main

pdf: $(MAIN).tex $(MAIN).bcf
	pdflatex main.tex
	biber main.bcf
	pdflatex main.tex

clean:
	rm -vf *.bbl *.bbg *.blg *.aux *.fls *.fdb_latexmk *.log *.out *.toc *.synctex.gz $(MAIN).pdf
