#ifndef __CURSOR_H
#define __CURSOR_H

#include"bufferManager.h"
/**
 * @brief The cursor is an important component of the system. To read from a
 * table, you need to initialize a cursor. The cursor reads rows from a page one
 * at a time.
 *
 */
class Cursor{
    public:
    Page page;
    int pageIndex;
    string tableName;
    int pagePointer;
    int bucket;
    int chainCount;
    public:
    Cursor();
    Cursor(string tableName, int pageIndex);
    Cursor(string tableName, int bucket, int chainCount);
    vector<int> getNext();    
    vector<int> getNextInBucket();
    vector<int> getNextInAllBuckets();    
    void nextPage(int pageIndex);
    void nextPage(int bucket, int chainCount);

};

#endif