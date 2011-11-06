#!/bin/sh
./client/client -port=9014 uc dga
./client/client -port=9014 uc bryant
./client/client -port=9014 uc rstarzl
./client/client -port=9014 uc imoraru

./client/client -port=9014 sl dga
./client/client -port=9014 sa dga bryant
./client/client -port=9014 sl dga
./client/client -port=9014 sa dga imoraru
./client/client -port=9014 sl dga

./client/client -port=9014 tl dga
./client/client -port=9014 tp dga "First post"
./client/client -port=9014 tl dga
./client/client -port=9014 tp dga "Second post"
./client/client -port=9014 tl dga

./client/client -port=9014 sa bryant imoraru
./client/client -port=9014 sa bryant dga
./client/client -port=9014 tp imoraru "Iulian's first post"
./client/client -port=9014 tp imoraru "Iulian's second post"
./client/client -port=9014 ts bryant
