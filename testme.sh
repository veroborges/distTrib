#!/bin/sh
./client/client uc dga
./client/client uc bryant
./client/client uc rstarzl
./client/client uc imoraru

./client/client sl dga
./client/client sa dga bryant
./client/client sl dga
./client/client sa dga imoraru
./client/client sl dga

./client/client tl dga
./client/client tp dga "First post"
./client/client tl dga
./client/client tp dga "Second post"
./client/client tl dga

./client/client sa bryant imoraru
./client/client sa bryant dga
./client/client tp imoraru "Iulian's first post"
./client/client tp imoraru "Iulian's second post"
./client/client ts bryant
