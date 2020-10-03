#include "global.h"

BufferManager::BufferManager() {
    logger.log("BufferManager::BufferManager");
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page BufferManager::getPage(string tableName, int pageIndex) {
    logger.log("BufferManager::getPage");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
        return this->getFromPool(pageName);
    else
        return this->insertIntoPool(tableName, pageIndex);
}

/**
 * @brief Function called to read a matrix page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page BufferManager::getMatrixPage(const string& matrixName, int rowIndex, int colIndex) {
    logger.log("BufferManager::getMatrixPage");

    string pageName = "../data/temp/" + matrixName + "_Page_" + to_string(rowIndex) + "_" + to_string(colIndex);
    if (this->inPool(pageName)) {
        return this->getFromPool(pageName);
    } else {
        return this->insertMatrixPageIntoPool(matrixName, rowIndex, colIndex);
    }
}

/**
 * @brief Function called to read a linear hash page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page BufferManager::getHashPage(const string& tableName, int bucket, int chainCount) {
    logger.log("BufferManager::getHashPage");

    string pageName = "../data/temp/" + tableName + "_Page_" + to_string(bucket) + "_" + to_string(chainCount);
    if (this->inPool(pageName)) {
        return this->getFromPool(pageName);
    } else {
        return this->insertIntoPool(tableName, bucket, chainCount);
    }
}

/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName 
 * @return true 
 * @return false 
 */
bool BufferManager::inPool(string pageName) {
    logger.log("BufferManager::inPool");
    for (auto page : this->pages) {
        if (pageName == page.pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName 
 * @return Page 
 */
Page BufferManager::getFromPool(string pageName) {
    logger.log("BufferManager::getFromPool");
    for (auto page : this->pages)
        if (pageName == page.pageName)
            return page;
}

/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure. 
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page BufferManager::insertIntoPool(string tableName, int pageIndex) {
    logger.log("BufferManager::insertIntoPool");
    Page page(tableName, pageIndex);
    if (this->pages.size() >= BLOCK_COUNT)
        pages.pop_front();
    pages.push_back(page);
    return page;
}

/**
 * @brief Inserts linear hashed page indicated by tableName, bucket and chainCount into pool.
 * If the pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure. 
 *
 * @param tableName 
 * @param bucket
 * @param chainCount
 *  
 * @return Page 
 */
Page BufferManager::insertHashPageIntoPool(const string* tableName, int bucket, int chainCount) {
    logger.log("BufferManager::insertHashPageIntoPool");
    Page page(tableName, bucket, chainCount);
    if (this->pages.size() >= BLOCK_COUNT)
        this->pages.pop_front();
    this->pages.push_back(page);
    return page;
}

/**
 * @brief Inserts linear hashed page indicated by matrixName, rowIndex and colIndex into pool.
 * If the pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure. 
 *
 * @param matrixName
 * @param rowIndex
 * @param colIndex
 *  
 * @return Page 
 */
Page BufferManager::insertMatrixPageIntoPool(const string& matrixName, int rowIndex, int colIndex) {
    logger.log("BufferManager::insertMatrixPageIntoPool");

    Page page(matrixName, rowIndex, colIndex);
    if (this->pages.size() >= BLOCK_COUNT)
        this->pages.pop_front();
    this->pages.push_back(page);
    return page;
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements.
 *
 * @param tableName 
 * @param pageIndex 
 * @param rows 
 * @param rowCount 
 */
void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount) {
    logger.log("BufferManager::writePage");
    Page page(tableName, pageIndex, rows, rowCount);
    page.writePage();
}

void BufferManager::writeHashPage(const string& tableName, int bucket, int chainCount, const vector<vector<int>>& data) {
    logger.log("BufferManager::writeHashPage");
    Page page(tableName, bucket, chainCount, data);

    for (auto& p : this->pages) {
        if (p.pageName == page.pageName) {
            p = page;
            break;
        }
    }
    page.writePage(); // TODO: deferred writes
}

void BufferManager::writeMatrixPage(const string& matrixName, int rowIndex, int colIndex, const vector<vector<int>>& data) {
    logger.log("BufferManager::writeMatrixPage");
    Page page(matrixName, rowIndex, colIndex, data);

    for (auto& p : this->pages) {
        if (p.pageName == page.pageName) {
            p = page;
            break;
        }
    }

    page.writeMatrixPage();
}

/**
 * @brief Deletes file names fileName
 *
 * @param fileName 
 */
void BufferManager::deleteFile(string fileName) {
    if (remove(fileName.c_str()))
        logger.log("BufferManager::deleteFile: Err");
    else
        logger.log("BufferManager::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the tableName and pageIndex.
 *
 * @param tableName 
 * @param pageIndex 
 */
void BufferManager::deleteFile(string tableName, int pageIndex) {
    logger.log("BufferManager::deleteFile");
    string fileName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
}

void BufferManager::deleteFile(const string& matrixName, int rowIndex, int colIndex) {
    logger.log("BufferManager::deleteFile");

    string fileName = "../data/temp/" + matrixName + "_Page_" + to_string(rowIndex) + "_" + to_string(colIndex);
    this->deleteFile(fileName);
}