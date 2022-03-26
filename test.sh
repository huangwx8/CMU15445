#!/usr/bin/sh
 
for file in $(find *.sql); do
    run="sqlite3 imdb-cmudb2019.db"
    cat ${file} | ${run} > myans.txt
    cat 'hw1-sols/'${file} | ${run} > stdans.txt
    $(diff myans.txt stdans.txt)
    if [ $? != 0 ]
    then
        echo "wrong answer"
        return 1
    else
        echo "${file} passed"
    fi
done
