# Overview
**proj**: fast xml/json/log/csv queries without leaving the command line. 

This repo is quite new, and documentation is forthcoming.  Until then, please refer to the following resources:
1. A [11-step tutorial](https://github.com/arlettedata/proj/blob/master/tutorial/TUTORIAL.md).
2. An introductory/historical writeup in this repo's [wiki](https://github.com/arlettedata/proj/wiki/Proj.--The-initial-wiki-entry.).
3. [Unit tests](https://github.com/arlettedata/proj/tree/master/tests) you can scrape for examples.

In addition, there are unit tests that serve as examples.

# Build
The repo is built using g++ with C++11 support, and the resulting binary can be moved to a common location.

```
$ make release
$ cp proj /usr/local/bin
$ echo "<greeting>Hello, world</greeting>" | proj greeting
```

The last command above should output:
```
greeting
Hello, world
```
which can be treated as a CSV file.

# Test
Unit tests are included.  To run them, node.js must first be installed (assuming MacOS here):

```
$ brew install node
$ node tests
```

Running selected tests or directories containing tests can be done with:
```
$ node tests [path1] [path2] ... 
```

Each test is associated with an expected output or error.  To update them, use the `--rebase` flag:
```
$ node tests --rebase [path1] [path2] ... 
```

Stay tuned for a tutorial on some real world XML data...
