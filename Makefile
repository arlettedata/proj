init: FORCE
	npm install

tests: FORCE
	node tests
	
debug:
	mkdir -p bin
	g++ main.cpp -Ixml_lib -std=c++11 -o bin/proj

release:
	mkdir -p bin
	g++ main.cpp -Ixml_lib -std=c++11 -O2 -o bin/proj

deploy: release
	cp bin/proj /usr/local/bin

FORCE:
