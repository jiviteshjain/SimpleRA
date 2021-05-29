# SimpleRA

This is a simplified relational database management system which supports basic database operations on integer-only tables and load, store and transpose operations efficiently on arbitrarily large matrices. Supported indexing schemes are linear hash and B+ tree. Supported sorting schemes are two-phase on-disk merge sort, and in-memory buffers are managed by delaying writes. There is no support for transaction or thread safety.  

For more details, see accompanying documentation.

## Compilation Instructions

We use ```make``` to compile all the files and creste the server executable. ```make``` is used primarily in Linux systems, so those of you who want to use Windows will probably have to look up alternatives (I hear there are ways to install ```make``` on Windows). To compile

```cd``` into the SimpleRA directory
```
cd SimpleRA
```
```cd``` into the soure directory (called ```src```)
```
cd src
```
To compile
```
make clean
make
```

## To run

Post compilation, an executable names ```server``` will be created in the ```src``` directory
```
./server
```
---
*This software was written by [@jiviteshjain](https://github.com/jiviteshjain) and [@shanmukh1608](https://github.com/shanmukh1608) as part of the Data Systems, Monsoon 2020 course at IIIT Hyderabad, instructed by Professor Kamal Karpalem. It is meant to be a learning aid, and is not production-ready.*
