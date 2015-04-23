#!/bin/bash

rm -r html
rm -r doc
sphinx-apidoc . --full -o doc -H 'Pythians'
cp conf.py doc/conf.py 
cd doc
make html
cp -r _build/html ../html
cd ..
