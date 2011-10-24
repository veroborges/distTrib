#!/bin/sh
# Demonstrates some of the storage functionality
SC="./bin/storageclient"
echo "Let's get a key that doesn't exist.  Should fail."
$SC g nosuchkey
echo "Let's put and get a key back.  Both should succeed."
$SC p anewkey somevalue
$SC g anewkey
echo "Hey, a list.  Let's create one and fill it with some items."
$SC la alistkey item1
$SC la alistkey item2
$SC la alistkey item3
$SC lg alistkey
echo "Now let's remove that last item"
$SC lr alistkey item3
$SC lg alistkey
echo "And add a duplicate, which should return an exists error"
$SC la alistkey item2
