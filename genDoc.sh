rm -r doc
sphinx-apidoc . --full -o doc -H 'Pythians'
cp conf.py doc/conf.py 
cd doc
make html
cd ..
