#!/bin/bash

function fileExists () 
{
    if [ ! -f  "$1" ]
    then
        echo "$1 is not present"
        exit -1
    fi
}

fileExists "pythians.py"

python3 tests.py > tests.out 2>&1

if grep -q FAILED tests.out; then
    echo "Failed a test"
    cat tests.out
    exit -1
fi

bash genDoc.sh

# git config --global user.name "Travis CI"
# git config --global user.email "PatrickAupperle@gmail.com"
# git config --global push.default simple
# git config --global credential.helper store
