init: FORCE
	npm install

tests: FORCE
	node tests
	
debug:
	g++ main.cpp -Ixml_lib -std=c++11 -o proj

release:
	g++ main.cpp -Ixml_lib -std=c++11 -O2 -o proj

FORCE:
