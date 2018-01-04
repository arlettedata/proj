debug:
	mkdir -p bin
	clang++ main.cpp -Ixml_lib -std=c++11 -stdlib=libc++ -o bin/proj

release:
	mkdir -p bin
	clang++ main.cpp -Ixml_lib -std=c++11 -stdlib=libc++ -O2 -o bin/proj

deploy: release
	cp bin/proj /usr/local/bin

init: FORCE
	npm install

tests: FORCE
	node tests

FORCE:
