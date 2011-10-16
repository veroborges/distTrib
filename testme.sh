#!/bin/sh
./client/client -host unix33.andrew.cmu.edu -port 9009 uc dga
./client/client -host unix33.andrew.cmu.edu -port 9009 uc bryant
./client/client -host unix33.andrew.cmu.edu -port 9009 uc rstarzl
./client/client -host unix33.andrew.cmu.edu -port 9009 uc imoraru

./client/client -host unix33.andrew.cmu.edu -port 9009 sl dga
./client/client -host unix33.andrew.cmu.edu -port 9009 sa dga bryant
./client/client -host unix33.andrew.cmu.edu -port 9009 sl dga
./client/client -host unix33.andrew.cmu.edu -port 9009 sa dga imoraru
./client/client -host unix33.andrew.cmu.edu -port 9009 sl dga

./client/client -host unix33.andrew.cmu.edu -port 9009 tl dga
./client/client -host unix33.andrew.cmu.edu -port 9009 tp dga "First post"
./client/client -host unix33.andrew.cmu.edu -port 9009 tl dga
./client/client -host unix33.andrew.cmu.edu -port 9009 tp dga "Second post"
./client/client -host unix33.andrew.cmu.edu -port 9009 tl dga

./client/client -host unix33.andrew.cmu.edu -port 9009 sa bryant imoraru
./client/client -host unix33.andrew.cmu.edu -port 9009 sa bryant dga
./client/client -host unix33.andrew.cmu.edu -port 9009 tp imoraru "Iulian's first post"
./client/client -host unix33.andrew.cmu.edu -port 9009 tp imoraru "Iulian's second post"
./client/client -host unix33.andrew.cmu.edu -port 9009 ts bryant
