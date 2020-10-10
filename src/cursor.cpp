#include "global.h"

Cursor::Cursor(string tableName, int pageIndex) {
    logger.log("Cursor::Cursor");
    this->page = bufferManager.getTablePage(tableName, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
}

Cursor::Cursor(string tableName, int bucket, int chainCount) {
    logger.log("Cursor::Cursor");
    this->page = bufferManager.getHashPage(tableName, bucket, chainCount);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->bucket = bucket;
    this->chainCount = chainCount;
}

/**
 * @brief This function reads the next row from the page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int> 
 */
vector<int> Cursor::getNext() {
    logger.log("Cursor::getNext");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    if (result.empty()) {
        tableCatalogue.getTable(this->tableName)->getNextPage(this);
        if (!this->pagePointer) {
            result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
        }
    }
    return result;
}

/**
 * @brief This function reads the next row from the hash page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int> 
 */
vector<int> Cursor::getNextInBucket() {
    logger.log("Cursor::getNextInBucket");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    if (result.empty()) {
        tableCatalogue.getTable(this->tableName)->getNextPage(this, this->chainCount);
        if (!this->pagePointer) {
            result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
        }
    }
    return result;
}

/**
 * @brief This function reads the next row from the hash page of all buckets. 
 * The index of the current row read from the page is indicated by the 
 * pagePointer(points to row in page the cursor is pointing to). 
 *
 * @return vector<int> 
 */
vector<int> Cursor::getNextInAllBuckets() {
    logger.log("Cursor::getNextInAllBuckets");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    if (result.empty()) {
        tableCatalogue.getTable(this->tableName)->getNextPage(this, this->bucket, this->chainCount);
        if (!this->pagePointer) {
            result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
        }
    }
    return result;
}

/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page.
 *
 * @param pageIndex 
 */
void Cursor::nextPage(int pageIndex) {
    logger.log("Cursor::nextPage");
    this->page = bufferManager.getTablePage(this->tableName, pageIndex);
    this->pageIndex = pageIndex;
    this->pagePointer = 0;
}

/**
 * @brief Function that loads Page indicated by bucket and chainCount. Now the 
 * cursor starts reading from the new page.
 *
 * @param bucket
 * @param chainCount 
 */
void Cursor::nextPage(int bucket, int chainCount) {
    logger.log("Cursor::nextPage");
    this->page = bufferManager.getHashPage(this->tableName, bucket, chainCount);
    this->bucket = bucket;
    this->chainCount = chainCount;
    this->pagePointer = 0;
}