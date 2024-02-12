all = pyas p4thpymap

.PHONY: $(all)

all: $(all)

pyas: 
	(git clone https://github.com/positively4th/pyas.git || echo) \
	&& (cd pyas && git pull) \
	&& ([ -f "pyas/Makefile" ] && make -C pyas || echo)

p4thpymisc: 
	(git clone https://github.com/positively4th/p4thpymisc.git || echo) \
	&& (cd p4thpymisc && git pull) \
	&& ([ -f "p4thpymisc/Makefile" ] && make -C p4thpymisc || echo)

p4thpymap: 
	(git clone https://github.com/positively4th/p4thpymap.git || echo) \
	&& (cd p4thpymap && git pull) \
	&& ([ -f "p4thcson/Makefile" ] && make -C p4thpymap || echo)


clean: 
	rm -rf $(all)


