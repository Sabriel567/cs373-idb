#!/bin/bash

function fileExists () 
{
    if [ ! -f  "$1" ]
    then
        echo "$1 is not present"
        exit -1
    fi
}

python3 tests.py > tests.out 2>&1

if grep -q FAILED tests.out; then
    echo "Failed a test"
    cat tests.out
    exit -1
fi

bash genDoc.sh


fileExists "pythians.py"
fileExists "apiary.apib"
fileExists "models.html"
fileExists "tests.out"
fileExists "tests.py"
fileExists "UML.pdf"

git config --global user.name "Travis CI"
git config --global user.email "PatrickAupperle@gmail.com"

git add tests.out
git add html

git commit -m "Genereated HTML"

git log > IDB.log
git add IDB.log

git commit -m "Added Log"

git push --force --quiet "https://${DEPLOY_KEY}@github.com/Sabriel567/cs373-idb.git" HEAD:travis
