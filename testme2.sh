#!/bin/sh
./client/client -port=9011 uc dga
./client/client -port=9011 uc bryant
./client/client -port=9011 uc rstarzl
./client/client -port=9011 uc imoraru

./client/client -port=9011 sl dga
./client/client -port=9011 sa dga bryant
./client/client -port=9011 sl dga
./client/client -port=9011 sa dga imoraru
./client/client -port=9011 sl dga

./client/client -port=9011 tl dga
./client/client -port=9011 tp dga "First post"
./client/client -port=9011 tl dga
./client/client -port=9011 tp dga "Second post"
./client/client -port=9011 tl dga

./client/client -port=9011 sa bryant imoraru
./client/client -port=9011 sa bryant dga
./client/client -port=9011 tp imoraru "Iulian's first post"
./client/client -port=9011 tp imoraru "Iulian's second post"
./client/client -port=9011 ts bryant
