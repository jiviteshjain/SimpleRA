#include "global.h"

/**
 * @brief Comparator for binary search
 * 
 * @param left
 * 
 * @param right
 * 
 * @return bool 
 */
bool secondLessThan(const pair<int, int> &left, int right) {
    return left.second < right;
}

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table() {
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName 
 */
Table::Table(string tableName) {
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName 
 * @param columns 
 */
Table::Table(string tableName, vector<string> columns) {
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableName = tableName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    // this->valuesInColumns = vector<map<int, long long>> (this->columnCount);
    this->smallestInColumns = vector<int>(this->columnCount);
    this->largestInColumns = vector<int>(this->columnCount);
    // this->maxRowsPerBlock = 3; // DEBUG
    // this->writeRow<string>(columns);
}

/**
 * @brief Calculates density of linear hash blocks
 * 
 * @param offsetRows
 * 
 * @param offsetBlocks
 * 
 * @return density
 */
inline float Table::density(int offsetRows, int offsetBlocks) {
    return ((float)(this->rowCount + offsetRows)) / ((this->blockCount + offsetBlocks) * this->maxRowsPerBlock);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded 
 * @return false if an error occurred 
 */
bool Table::load() {
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line)) {
        fin.close();
        if (this->extractColumnNames(line))
            if (this->blockify())
                return true;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file. 
 *
 * @param line 
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine) {
    logger.log("Table::extractColumnNames");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ',')) {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (columnNames.count(word))
            return false;
        columnNames.insert(word);
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    // this->maxRowsPerBlock = 3; // DEBUG
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify() {
    logger.log("Table::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->columnCount, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    // this->valuesInColumns = vector<map<int, long long>> (this->columnCount);
    this->smallestInColumns = vector<int>(this->columnCount);
    this->largestInColumns = vector<int>(this->columnCount);
    getline(fin, line);
    while (getline(fin, line)) {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++) {
            if (!getline(s, word, ','))
                return false;
            row[columnCounter] = stoi(word);
            rowsInPage[pageCounter][columnCounter] = row[columnCounter];
        }
        pageCounter++;
        this->updateStatistics(row);
        if (pageCounter == this->maxRowsPerBlock) {
            bufferManager.writeTablePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }
    }
    if (pageCounter) {
        bufferManager.writeTablePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(pageCounter);
        pageCounter = 0;
    }

    if (this->rowCount == 0)
        return false;

    return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row 
 */
void Table::updateStatistics(vector<int> row) {
    logger.log("Table::updateStatistics");
    this->rowCount++;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++) {
        // this->valuesInColumns[columnCounter][row[columnCounter]]++;
        if (row[columnCounter] < smallestInColumns[columnCounter])
            smallestInColumns[columnCounter] = row[columnCounter];
        if (row[columnCounter] > largestInColumns[columnCounter])
            largestInColumns[columnCounter] = row[columnCounter];
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName 
 * @return true 
 * @return false 
 */
bool Table::isColumn(string columnName) {
    logger.log("Table::isColumn");
    for (auto col : this->columns) {
        if (col == columnName) {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName 
 * @param toColumnName 
 */
void Table::renameColumn(string fromColumnName, string toColumnName) {
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++) {
        if (columns[columnCounter] == fromColumnName) {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

/**
 * @brief Returns the name of the indexed column. For external use, member methods are
 * advised to directly access the internal data.
 *
 * @return indexed column name
 */
string Table::getIndexedColumn() {
    if (this->indexingStrategy == NOTHING) {
        return "";
    }
    return this->columns[this->indexedColumn];
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print() {
    logger.log("Table::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    //print headings
    this->writeRow(this->columns, cout);

    if (this->indexingStrategy == NOTHING) {
        Cursor cursor(this->tableName, 0);
        vector<int> row;
        for (int rowCounter = 0; rowCounter < count; rowCounter++) {
            row = cursor.getNext();
            this->writeRow(row, cout);
        }
    } else if (this->indexingStrategy == HASH || this->indexingStrategy == BTREE) {
        Cursor cursor = this->getCursor(0, 0);
        vector<int> row;
        for (int i = 0; i < count; i++) {
            row = cursor.getNextInAllBuckets();
            this->writeRow(row, cout);
        }
        // TODO: rewrite previous block like this
    }

    printRowCount(this->rowCount);
}

/**
 * @brief This function checks if cursor pageIndex is less than the table pageIndex, 
 * and calls the nextPage function of the cursor with incremented pageIndex.
 *
 * @param cursor 
 * @return vector<int> 
 */
void Table::getNextPage(Cursor *cursor) {
    logger.log("Table::getNextPage");

    if (cursor->pageIndex < (int)this->blockCount - 1) {
        cursor->nextPage(cursor->pageIndex + 1);
    }
}

/**
 * @brief This function checks if cursor chainCount is less than the bucket chainCount, 
 * and calls the nextPage function of the cursor with incremented chainCount.
 *
 * @param cursor 
 * @return void 
 */
void Table::getNextPage(Cursor *cursor, int chainCount) {
    logger.log("Table::getNextPage");
    if (cursor->chainCount < this->blocksInBuckets[cursor->bucket].size() - 1) {
        cursor->nextPage(cursor->bucket, cursor->chainCount + 1);
    }
}

/**
 * @brief This function checks if cursor chainCount is less than the bucket chainCount, 
 * and calls the nextPage function of the cursor with incremented chainCount. If the
 * bucket has been fully read, it calls the nextPage function of the cursor with
 * incremented bucketCount.
 *
 * @param cursor 
 * @return void 
 */
void Table::getNextPage(Cursor *cursor, int bucket, int chainCount) {
    logger.log("Table::getNextPage");

    if (cursor->chainCount < this->blocksInBuckets[cursor->bucket].size() - 1)
        cursor->nextPage(cursor->bucket, cursor->chainCount + 1);
    else {
        int bucket = -1;
        for (int i = cursor->bucket + 1; i < this->blocksInBuckets.size(); i++)
            if (this->blocksInBuckets[i].size()) {
                bucket = i;
                break;
            }

        if (bucket != -1)
            cursor->nextPage(bucket, 0);
    }
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent() {
    logger.log("Table::makePermanent");
    if (!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    //print headings
    this->writeRow(this->columns, fout);
    Cursor cursor;

    if (!this->indexed)
        cursor = this->getCursor();
    else if (this->indexingStrategy == HASH || this->indexingStrategy == BTREE)
        cursor = this->getCursor(0, 0);

    vector<int> row;
    for (long long rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
        if (!this->indexed)
            row = cursor.getNext();
        else if (this->indexingStrategy == HASH || this->indexingStrategy == BTREE)
            row = cursor.getNextInAllBuckets();
        this->writeRow(row, fout);
    }
    fout.close();
}

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent() {
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
        return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload() {
    logger.log("Table::~unload");
    if (this->indexingStrategy == NOTHING) {
        for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
            bufferManager.deleteTableFile(this->tableName, pageCounter);
        if (!isPermanent())
            bufferManager.deleteFile(this->sourceFileName);
    } else if (this->indexingStrategy == HASH || this->indexingStrategy == BTREE) {
        for (int i = 0; i < this->blocksInBuckets.size(); i++) {  // not necessarily this->M
            for (int j = 0; j < this->blocksInBuckets[i].size(); j++) {
                bufferManager.deleteHashFile(this->tableName, i, j);
            }
        }
    }
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 * 
 * @return Cursor 
 */
Cursor Table::getCursor() {
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}

/**
 * @brief Function that returns a cursor that reads rows from the blocks of a bucket 
 * @param bucket
 * @param chainCount 
 * @return Cursor 
 */
Cursor Table::getCursor(int bucket, int chainCount) {
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, bucket, chainCount);
    return cursor;
}

/**
 * @brief Function that returns the index of column indicated by columnName
 * 
 * @param columnName 
 * @return int 
 */
int Table::getColumnIndex(string columnName) {
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++) {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
}

// LINEAR HASHING

/**
 * @brief The hash function for linear hashing. Takes care of this table's M and N.
 * 
 * @param key
 * 
 * @return int
 */
int Table::hash(int key) {
    logger.log("Table::hash");

    int first = MOD(key, this->M);
    return (first < this->N ? MOD(key, 2 * M) : first);
}

/**
 * @brief Function that creates a linear hash index on the table
 * 
 * @param columnName
 * @param bucketCount
 * 
 * @return void
 */
void Table::linearHash(const string &columnName, int bucketCount) {
    logger.log("Table::linearHash");

    this->clearIndex();

    if (this->rowCount == 0) {
        return;
    }
    // cout << "DEBUG" << this->rowCount; // TODO: Check for correctness

    // set metadata
    this->indexed = true;
    this->indexedColumn = this->getColumnIndex(columnName);
    this->indexingStrategy = HASH;

    this->M = bucketCount;
    this->initialBucketCount = bucketCount;
    this->blocksInBuckets = vector<vector<int>>(this->M);

    int col = this->indexedColumn;
    Cursor cursor(this->tableName, 0);

    vector<int> row;
    for (long long i = 0; i < this->rowCount; i++) {
        row = cursor.getNext();
        int bucket = this->hash(row[col]);
        this->insertIntoHashBucket(row, bucket);
        // This would return if overflow or not,
        // but we don't need to split right now.
    }

    // Delete existing pages
    for (int i = 0; i < this->blockCount; i++) {
        bufferManager.deleteTableFile(this->tableName, i);
    }

    // Update block count
    this->blockCount = 0;
    for (auto &b : this->blocksInBuckets) {
        this->blockCount += b.size();
    }
    this->rowsPerBlockCount.clear();  // this doesn't hold any meaning anymore
}

/**
 * @brief Inserts a row into the last block of the overflow chain of a bucket. Allocates a new block if needed.
 * 
 * @param row
 * @param bucket
 * 
 * @return bool indicating an overflow
 */

bool Table::insertIntoHashBucket(const vector<int> &row, int bucket) {
    logger.log("Table::insertIntoHashBucket");
    if (bucket >= this->blocksInBuckets.size()) {
        return false;  // TODO: raise an error somehow
    }

    bool found = false;

    vector<vector<int>> rows(0);
    int chainCount;

    for (chainCount = 0; chainCount < this->blocksInBuckets[bucket].size(); chainCount++) {
        if (blocksInBuckets[bucket][chainCount] < this->maxRowsPerBlock) {
            rows = bufferManager.getHashPage(this->tableName, bucket, chainCount).data;
            this->blocksInBuckets[bucket][chainCount]++;

            found = true;
            break;
        }
    }

    if (!found) {
        this->blocksInBuckets[bucket].push_back(1);
        this->blockCount++;
        chainCount = blocksInBuckets[bucket].size() - 1;
    }

    rows.push_back(row);
    bufferManager.writeHashPage(this->tableName, bucket, chainCount, rows);

    return !found;
}

/**
 * @brief Inserts a row into the table
 * 
 * @param row
 * @param
 * 
 * @return void
 */
bool Table::insert(const vector<int> &row) {
    logger.log("Table::insert");
    if (row.size() != this->columnCount) {
        return false;
    }

    if (this->indexingStrategy == NOTHING) {
        vector<vector<int>> rows;
        int blockIndex;
        for (blockIndex = 0; blockIndex < this->blockCount; blockIndex++) {
            if (this->rowsPerBlockCount[blockIndex] != this->maxRowsPerBlock) {
                rows = bufferManager.getTablePage(this->tableName, blockIndex).data;
                // rows.resize(this->rowsPerBlockCount[blockIndex]);
                rows.push_back(row);
                // bufferManager.deleteTableFile(this->tableName, blockIndex);
                bufferManager.writeTablePage(this->tableName, blockIndex, rows, rows.size());
                this->rowsPerBlockCount[blockIndex] = rows.size();
                break;
            }
        }

        if (blockIndex == this->blockCount) {
            rows.push_back(row);
            bufferManager.writeTablePage(this->tableName, this->blockCount, rows, rows.size());
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(rows.size());
        }

    } else if (this->indexingStrategy == HASH) {
        int bucket = this->hash(row[this->indexedColumn]);
        // because row is large enough, this is always in bounds
        this->insertIntoHashBucket(row, bucket);

        // split
        // rowCount is updated later, so +1
        // we want to compare it with the original blockCount
        if (this->density(1) > HASH_DENSITY_MAX) {
            this->linearHashSplit();
        }

    } else if (this->indexingStrategy == BTREE) {
        int key = row[this->indexedColumn];  // always in bounds
        auto record = this->bTree.find(key, nullptr);

        bool overflow;
        int bucket;
        if (record != nullptr) {
            // this key already exists
            // bucketRanges, hence, do not need to be updated
            bucket = record->val();
            overflow = this->insertIntoHashBucket(row, bucket);
        } else {
            // key doesn't already exist
            // binary search a place for it
            auto potential = lower_bound(this->bucketRanges.begin(), this->bucketRanges.end(), key, secondLessThan);

            if (potential == this->bucketRanges.end()) {
                // insert in the last block
                bucket = this->blocksInBuckets.size() - 1;
            } else {
                // insert in potential
                bucket = potential - this->bucketRanges.begin();
            }

            this->bTree.insert(key, bucket);
            overflow = this->insertIntoHashBucket(row, bucket);

            // update bucket ranges
            this->bucketRanges[bucket].first = min(this->bucketRanges[bucket].first, key);
            this->bucketRanges[bucket].second = max(this->bucketRanges[bucket].second, key);
        }

        // On overflow, consider reindexing
        if (overflow) {
            if (blocksInBuckets[bucket].size() > 2 * blocksInBuckets.size() + 1) {
                int colIndex = this->indexedColumn;
                int fanout = this->bTree.ord();

                this->clearIndex();
                this->bTreeIndex(this->columns[colIndex], fanout);
            }
        }
    }

    // Update metadata
    this->updateStatistics(row);

    return true;
}

/**
 * @brief Splits the blocks of linearly hashed table on overflow
 * 
 * @param 
 * 
 * @return 
 */
void Table::linearHashSplit() {
    logger.log("Table::linearHashSplit");
    this->blocksInBuckets.push_back(vector<int>(0));  // M + N
    this->blocksInBuckets.push_back(vector<int>(0));  // M + N + 1 (temporary)

    // rehash
    Cursor cursor = this->getCursor(this->N, 0);
    auto row = cursor.getNextInBucket();
    while (!row.empty()) {
        int hashkey = row[this->indexedColumn];
        int hashval = MOD(hashkey, 2 * this->M);

        if (hashval == this->N) {
            this->insertIntoHashBucket(row, this->M + this->N + 1);
        } else {
            this->insertIntoHashBucket(row, this->M + this->N);
        }

        row = cursor.getNextInBucket();
    }

    // delete original buckets
    for (int i = 0; i < this->blocksInBuckets[this->N].size(); i++) {
        bufferManager.deleteHashFile(this->tableName, this->N, i);
    }

    // rename buckets
    for (int i = 0; i < this->blocksInBuckets.back().size(); i++) {
        auto data = bufferManager.getHashPage(this->tableName, this->M + this->N + 1, i).data;
        bufferManager.deleteHashFile(this->tableName, this->M + this->N + 1, i);

        bufferManager.writeHashPage(this->tableName, this->N, i, data);
    }

    // fix metadata
    this->blocksInBuckets[this->N] = this->blocksInBuckets.back();
    this->blocksInBuckets.pop_back();

    this->blockCount = 0;
    for (auto &b : this->blocksInBuckets) {
        this->blockCount += b.size();
    }

    this->N++;
    if (this->N == this->M) {
        this->M = this->M * 2;
        this->N = 0;
    }
}

/**
 * @brief Removes zero sized buckets
 * 
 * @param 
 * 
 * @return 
 */
void Table::cleanupBlocks(int bucket) {
    logger.log("Table::cleanupBlocks");
    if (this->blocksInBuckets[bucket].size() == 0) {
        return;
    }

    int i = 0, j = this->blocksInBuckets[bucket].size() - 1;
    while (i < j) {
        if (this->blocksInBuckets[bucket][i] > 0) {
            i++;
            continue;
        }
        if (this->blocksInBuckets[bucket][j] == 0) {
            j--;
            continue;
        }
        // swap i and j
        auto data = bufferManager.getHashPage(this->tableName, bucket, j).data;
        bufferManager.deleteHashFile(this->tableName, bucket, j);
        bufferManager.writeHashPage(this->tableName, bucket, i, data);
        this->blocksInBuckets[bucket][i] = this->blocksInBuckets[bucket][j];
        this->blocksInBuckets[bucket][j] = 0;

        i++;
        j--;
    }

    while (this->blocksInBuckets[bucket].size() > 0) {
        if (this->blocksInBuckets[bucket].back() == 0) {
            this->blocksInBuckets[bucket].pop_back();
        } else {
            break;
        }
    }

    this->blockCount = 0;
    for (auto &b : this->blocksInBuckets) {
        this->blockCount += b.size();
    }
}

/**
 * @brief Clears the index, if it exists, on the table
 * 
 * @param 
 * 
 * @return 
 */
void Table::clearIndex() {
    logger.log("Table::clearIndex");
    if (!this->indexed)
        return;

    // FOR BOTH B+TREE AND HASH

    int blocksWritten = 0;
    this->rowsPerBlockCount.clear();
    this->rowCount = 0;

    Cursor cursor = this->getCursor(0, 0);
    vector<int> row = cursor.getNextInAllBuckets();
    vector<vector<int>> rows;

    fill(this->smallestInColumns.begin(), this->smallestInColumns.end(), INT_MAX);
    fill(this->largestInColumns.begin(), this->largestInColumns.end(), INT_MIN);

    while (!row.empty()) {
        rows.push_back(row);
        this->updateStatistics(row);
        row = cursor.getNextInAllBuckets();
        if (rows.size() == this->maxRowsPerBlock) {
            this->rowsPerBlockCount.emplace_back(rows.size());
            bufferManager.writeTablePage(this->tableName, blocksWritten++, rows, rows.size());
            rows.clear();
        }
    }

    if (rows.size()) {
        this->rowsPerBlockCount.emplace_back(rows.size());
        bufferManager.writeTablePage(this->tableName, blocksWritten++, rows, rows.size());
        rows.clear();
    }

    for (int bucket = 0; bucket < this->blocksInBuckets.size(); bucket++)
        for (int chainCount = 0; chainCount < this->blocksInBuckets[bucket].size(); chainCount++)
            bufferManager.deleteHashFile(this->tableName, bucket, chainCount);

    this->blocksInBuckets.clear();
    if (this->indexingStrategy == HASH) {
        this->M = 0;
        this->N = 0;
        this->initialBucketCount = 0;
    } else {
        this->bTree.destroy_tree();
        this->bucketRanges.clear();
    }

    this->blockCount = blocksWritten;

    this->indexed = false;
    this->indexingStrategy = NOTHING;
    this->indexedColumn = -1;

    return;
}

/**
 * @brief Removes zero sized buckets
 * 
 * @param bucket
 * 
 * @return 
 */
void Table::mergeBlocks(int bucket) {
    logger.log("Table::mergeBlocks");
    bool foundAndFixed;
    do {
        foundAndFixed = false;
        for (int i = 0; i < this->blocksInBuckets[bucket].size(); i++) {
            if (this->blocksInBuckets[bucket][i] == this->maxRowsPerBlock) {
                continue;
            }
            if (this->blocksInBuckets[bucket][i] == 0) {
                continue;
            }
            for (int j = i + 1; j < this->blocksInBuckets[bucket].size(); j++) {
                if (this->blocksInBuckets[bucket][i] + this->blocksInBuckets[bucket][j] > this->maxRowsPerBlock) {
                    continue;
                }
                if (this->blocksInBuckets[bucket][j] == 0) {
                    continue;
                }

                auto tail = bufferManager.getHashPage(this->tableName, bucket, j).data;
                bufferManager.deleteHashFile(this->tableName, bucket, j);

                auto head = bufferManager.getHashPage(this->tableName, bucket, i).data;
                head.insert(head.end(), tail.begin(), tail.end());

                bufferManager.writeHashPage(this->tableName, bucket, i, head);
                this->blocksInBuckets[bucket][i] += this->blocksInBuckets[bucket][j];
                this->blocksInBuckets[bucket][j] = 0;

                foundAndFixed = true;
                break;
            }

            if (foundAndFixed) {
                break;
            }
        }
    } while (foundAndFixed);
}

/**
 * @brief Deletes a row, if exists, from the table
 * 
 * @param row
 * 
 * @return bool true if the row was deleted
 */
bool Table::remove(const vector<int> &row) {
    logger.log("Table::remove");
    // invalid row
    if (row.size() != this->columnCount) {
        return false;
    }

    for (int i = 0; i < this->columnCount; i++)
        if (this->smallestInColumns[i] > row[i] || this->largestInColumns[i] < row[i])
            return false;

    long long foundCount = 0;

    if (this->indexingStrategy == NOTHING) {
        vector<vector<int>> data;
        vector<vector<int>>::iterator it;
        int pageIndex;

        for (int pageIndex = 0; pageIndex < this->blockCount; pageIndex++) {
            long long foundInPage = 0;

            data = bufferManager.getTablePage(this->tableName, pageIndex).data;
            it = data.begin();

            while (it != data.end()) {
                if (*it == row) {
                    it = data.erase(it);
                    foundInPage++;
                } else
                    it++;
            }

            if (foundInPage > 0) {
                this->rowsPerBlockCount[pageIndex] = data.size();

                if (data.size() == 0)
                    bufferManager.deleteTableFile(this->tableName, pageIndex);
                else
                    bufferManager.writeTablePage(this->tableName, pageIndex, data, data.size());

                foundCount = foundCount + foundInPage;
            }
        }
        if (foundCount) {
            bool foundAndFixed;
            do {
                foundAndFixed = false;
                for (int i = 0; i < this->blockCount; i++) {
                    if (this->rowsPerBlockCount[i] == this->maxRowsPerBlock || this->rowsPerBlockCount[i] == 0)
                        continue;

                    for (int j = i + 1; j < this->blockCount; j++) {
                        if (this->rowsPerBlockCount[i] + this->rowsPerBlockCount[j] > this->maxRowsPerBlock || this->rowsPerBlockCount[j] == 0)
                            continue;

                        auto tail = bufferManager.getTablePage(this->tableName, j).data;
                        bufferManager.deleteTableFile(this->tableName, j);

                        auto head = bufferManager.getTablePage(this->tableName, i).data;
                        head.insert(head.end(), tail.begin(), tail.end());

                        bufferManager.writeTablePage(this->tableName, i, head, head.size());
                        this->rowsPerBlockCount[i] += this->rowsPerBlockCount[j];
                        this->rowsPerBlockCount[j] = 0;
                        foundAndFixed = true;
                        break;
                    }

                    if (foundAndFixed)
                        break;
                }
            } while (foundAndFixed);

            while (this->blockCount > 0) {
                if (this->rowsPerBlockCount[blockCount - 1] == 0) {
                    bufferManager.deleteTableFile(this->tableName, this->blockCount - 1);
                    this->blockCount--;
                } else
                    break;
            }

            for (int i = 0; i < this->blockCount - 1; i++)  // remove 0 size blocks
            {
                if (this->rowsPerBlockCount[i] == 0) {
                    vector<vector<int>> finalBlockRows = bufferManager.getTablePage(this->tableName, this->blockCount - 1).data;
                    bufferManager.deleteTableFile(this->tableName, this->blockCount - 1);
                    bufferManager.writeTablePage(this->tableName, i, finalBlockRows, finalBlockRows.size());
                    this->rowsPerBlockCount[i] = finalBlockRows.size();
                    this->rowsPerBlockCount[this->blockCount - 1] = 0;
                    this->blockCount--;
                }
            }
        }
        this->rowCount = this->rowCount - foundCount;

    } else if (this->indexingStrategy == HASH) {
        // check corresponding bucket
        // does not use cursor because have to modify page if row was actually found
        int bucket = this->hash(row[this->indexedColumn]);

        // to avoid repeated constructor and destructor calls
        vector<vector<int>> data;
        vector<vector<int>>::iterator it;

        for (int i = 0; i < this->blocksInBuckets[bucket].size(); i++) {
            long long foundInPage = 0;

            data = bufferManager.getHashPage(this->tableName, bucket, i).data;
            it = data.begin();

            while (it != data.end()) {
                if (*it == row) {
                    it = data.erase(it);
                    foundInPage++;
                } else {
                    it++;
                }
            }

            if (foundInPage > 0) {
                this->blocksInBuckets[bucket][i] = data.size();

                if (data.size() > 0) {
                    bufferManager.writeHashPage(this->tableName, bucket, i, data);
                } else {
                    bufferManager.deleteHashFile(this->tableName, bucket, i);
                }

                foundCount = foundCount + foundInPage;
            }
        }

        if (foundCount > 0) {
            this->rowCount = this->rowCount - foundCount;

            // clean up pages of size 0
            this->cleanupBlocks(bucket);  // not doing merge blocks to let underflow do it

            // combine only on underflow
            if (this->density() < HASH_DENSITY_MIN) {
                this->linearHashCombine();
            }
        }

    } else {  // B+Tree

        // check corresponding bucket
        // does not use cursor because have to modify page if row was actually found
        int key = row[this->indexedColumn];
        auto record = this->bTree.find(key, nullptr);  // not out of range
        if (record == nullptr) {
            return false;
        }
        int bucket_i = record->val();
        int bucket_f = bucket_i;
        while (bucket_f < this->blocksInBuckets.size()) {
            if (this->bucketRanges[bucket_f].first <= key) {
                bucket_f++;
            } else {
                break;
            }
        }
        this->bTree.del(key);  // will be inserted later if reqd
        // donot use record after this

        for (int bucket = bucket_f - 1; bucket >= bucket_i; bucket--) {
            long long foundInBucketCount = 0;

            int minn = INT_MAX, maxx = INT_MIN;
            bool anotherOneExists = false;     // same key different row exists
            bool somethingElseExists = false;  // the bucket is not empty after deletion

            vector<vector<int>> data;
            vector<vector<int>>::iterator it;
            // to avoid repeated constructor and destructor calls
            for (int i = 0; i < this->blocksInBuckets[bucket].size(); i++) {
                long long foundInPage = 0;

                data = bufferManager.getHashPage(this->tableName, bucket, i).data;
                it = data.begin();

                while (it != data.end()) {
                    if (*it == row) {
                        it = data.erase(it);
                        foundInPage++;
                    } else {
                        minn = min(minn, it->operator[](this->indexedColumn));
                        maxx = max(maxx, it->operator[](this->indexedColumn));
                        anotherOneExists |= (it->operator[](this->indexedColumn) == key);
                        somethingElseExists = true;
                        it++;
                    }
                }

                if (foundInPage > 0) {
                    this->blocksInBuckets[bucket][i] = data.size();

                    if (data.size() > 0) {
                        bufferManager.writeHashPage(this->tableName, bucket, i, data);
                    } else {
                        bufferManager.deleteHashFile(this->tableName, bucket, i);
                    }

                    foundInBucketCount = foundInBucketCount + foundInPage;
                }
            }

            if (anotherOneExists) {
                this->bTree.insert(key, bucket);
            }

            if (foundInBucketCount > 0) {
                this->rowCount = this->rowCount - foundInBucketCount;

                if (somethingElseExists) {
                    this->bucketRanges[bucket] = make_pair(minn, maxx);
                }

                // if (!anotherOneExists) {
                //     this->bTree.del(row[this->indexedColumn]);
                // }

                // merge blocks that can be merged
                this->mergeBlocks(bucket);

                // clean up pages of size 0
                this->cleanupBlocks(bucket);

                foundCount += foundInBucketCount;
            }
        }

        // under extreme circumstances, consider reindexing
        if (this->density() < REINDEX_MIN_THRESH) {
            int colIndex = this->indexedColumn;
            int fanout = this->bTree.ord();

            this->clearIndex();
            this->bTreeIndex(this->columns[colIndex], fanout);
        }
    }

    if (this->rowCount == 0)
        tableCatalogue.deleteTable(this->tableName);

    return (foundCount != 0);
}

/**
 * @brief Merges buckets if their combined capacity fits in a block (underflow)
 * 
 * @param 
 * 
 * @return 
 */
void Table::linearHashCombine() {
    logger.log("Table::linearHashCombine");
    // can't combine further, M could be odd
    if (this->M == this->initialBucketCount && this->N == 0) {
        return;
    }

    // handle the special case of undoing doubling of M
    if (this->N == 0) {
        this->M /= 2;
        this->N = this->M;
    }

    // copy rows back
    int to = this->N - 1;
    int from = to + this->M;

    Cursor cursor = this->getCursor(from, 0);
    auto row = cursor.getNextInBucket();
    while (!row.empty()) {
        this->insertIntoHashBucket(row, to);
        row = cursor.getNextInBucket();
    }

    // delete the bucket
    for (int i = 0; i < this->blocksInBuckets[from].size(); i++) {
        bufferManager.deleteHashFile(this->tableName, from, i);
    }

    // fix metadata
    this->N--;
    this->blocksInBuckets.pop_back();  // has to be the last entry

    this->blockCount = 0;
    for (auto &b : this->blocksInBuckets) {
        this->blockCount += b.size();
    }
}

/**
 * @brief Sorts a table in-place, used for B+-tree
 * 
 * @param bufferSize
 * 
 * @param columnName
 * 
 * @param capacity
 * 
 * @param sortingStrategy
 * 
 * @return 
 */
void Table::sort(int bufferSize, string columnName, float capacity, int sortingStrategy) {
    logger.log("Table::sort");
    int runSize = this->maxRowsPerBlock * bufferSize;
    Cursor cursor = this->getCursor();
    vector<int> row = cursor.getNext();

    int columnIndex = this->getColumnIndex(columnName);
    // TODO: change this to use columnindex directly
    int runsCount = 0;
    int zerothPassRunsCount;
    unordered_map<int, int> pagesInRun;

    int originalMaxRowsPerBlock = this->maxRowsPerBlock;
    int originalBlockCount = this->blockCount;

    int newMaxRowsPerBlock = floor(this->maxRowsPerBlock * capacity);
    int newBlockCount = ceil(this->rowCount * 1.0 / newMaxRowsPerBlock * 1.0);
    this->blocksInBuckets.resize(newBlockCount);
    this->rowsPerBlockCount.resize(newBlockCount + 2 * originalBlockCount);

    int blocksWritten = 0;
    while (!row.empty()) {
        vector<vector<int>> rows;
        int rowsRead = 0;
        while (!row.empty() && rowsRead != runSize) {
            rows.push_back(row);
            row = cursor.getNext();
            rowsRead++;
        }

        if (rows.size())
            std::sort(rows.begin(), rows.end(),
                      [&](const vector<int> &a, const vector<int> &b) {
                          if (sortingStrategy == 0)
                              return a[columnIndex] < b[columnIndex];
                          else if (sortingStrategy == 1)
                              return a[columnIndex] > b[columnIndex];
                      });

        int rowsWritten = 0;
        while (rowsWritten < rowsRead) {
            vector<vector<int>> subvector;
            if (rowsWritten + originalMaxRowsPerBlock <= rowsRead) {
                subvector = {rows.begin() + rowsWritten, rows.begin() + rowsWritten + originalMaxRowsPerBlock};
                rowsWritten += originalMaxRowsPerBlock;
            } else {
                subvector = {rows.begin() + rowsWritten, rows.end()};
                rowsWritten = rowsRead;
            }

            this->rowsPerBlockCount[newBlockCount + blocksWritten] = subvector.size();
            bufferManager.writeTablePage(this->tableName, newBlockCount + blocksWritten++, subvector, subvector.size());
            pagesInRun[runsCount]++;
        }
        runsCount++;
        zerothPassRunsCount = runsCount;
    }

    this->blockCount = newBlockCount + this->blockCount;

    int passCount = 0;
    int totalPasses = ceil(log(runsCount) / log(bufferSize - 1));

    int finalBlocksWritten = 0;
    while (passCount < totalPasses) {
        int runsRead = 0;
        int runsWritten = 0;
        int blocksWritten = 0;
        while (runsRead < runsCount) {
            unordered_map<int, int> pagesReadInRun;
            int minSize = min(runsCount - runsRead, bufferSize - 1);

            vector<vector<vector<int>>> dataFromPages(minSize);
            vector<int> rowsReadFromPages(minSize, 0);

            auto comp = [&](const pair<vector<int>, int> &a, const pair<vector<int>, int> &b) {
                if (sortingStrategy == 0)
                    return a.first[columnIndex] > b.first[columnIndex];
                else if (sortingStrategy == 1)
                    return a.first[columnIndex] < b.first[columnIndex];
            };

            priority_queue<pair<vector<int>, int>, vector<pair<vector<int>, int>>, function<bool(const pair<vector<int>, int> &a, const pair<vector<int>, int> &b)>> heap(comp);

            for (int i = 0; i < minSize; i++) {
                int runPageIndex = (passCount & 1 ? newBlockCount + originalBlockCount : newBlockCount);

                runPageIndex += ((runsRead + i) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + i)]++);
                dataFromPages[i] = (bufferManager.getTablePage(this->tableName, runPageIndex).data);
                // dataFromPages[i].resize(this->rowsPerBlockCount[runPageIndex]);
                heap.push(make_pair(dataFromPages[i][rowsReadFromPages[i]++], i));
            }
            vector<vector<int>> rows;
            while (!heap.empty()) {
                pair<vector<int>, int> temp = heap.top();
                heap.pop();

                rows.push_back(temp.first);
                if (rows.size() == newMaxRowsPerBlock && passCount == totalPasses - 1) {
                    this->blocksInBuckets[finalBlocksWritten].emplace_back(rows.size());
                    bufferManager.writeHashPage(this->tableName, finalBlocksWritten++, 0, rows);
                    // bufferManager.writeTablePage(this->tableName, finalBlocksWritten++, rows, rows.size());
                    // this->rowsPerBlockCount[finalBlocksWritten] = rows.size();

                    pagesInRun[(passCount + 1) * zerothPassRunsCount + runsWritten]++;
                    rows.clear();
                }
                if (rows.size() == originalMaxRowsPerBlock && passCount != totalPasses - 1) {
                    int runPageIndex = (passCount & 1 ? newBlockCount : newBlockCount + originalBlockCount);
                    runPageIndex += blocksWritten++;

                    this->rowsPerBlockCount[runPageIndex] = rows.size();
                    bufferManager.writeTablePage(this->tableName, runPageIndex, rows, rows.size());
                    this->blockCount++;

                    pagesInRun[(passCount + 1) * zerothPassRunsCount + runsWritten]++;
                    rows.clear();
                }

                int runPageIndex = (passCount & 1 ? newBlockCount + originalBlockCount : newBlockCount);

                runPageIndex += ((runsRead + temp.second) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)] - 1);
                if (rowsReadFromPages[temp.second] != this->rowsPerBlockCount[runPageIndex])
                    heap.push(make_pair(dataFromPages[temp.second][rowsReadFromPages[temp.second]++], temp.second));
                else  //all rows exhausted
                {
                    if (pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)] != pagesInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)])  // all pages not exhausted
                    {
                        int runPageIndex = (passCount & 1 ? newBlockCount + originalBlockCount : newBlockCount);
                        runPageIndex += ((runsRead + temp.second) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)]++);
                        dataFromPages[temp.second] = bufferManager.getTablePage(this->tableName, runPageIndex).data;
                        // dataFromPages[temp.second].resize(this->rowsPerBlockCount[runPageIndex]);
                        rowsReadFromPages[temp.second] = 0;
                        heap.push(make_pair(dataFromPages[temp.second][rowsReadFromPages[temp.second]++], temp.second));
                    }
                }
            }
            if (rows.size()) {
                int runPageIndex = (passCount & 1 ? newBlockCount : newBlockCount + originalBlockCount);

                runPageIndex += blocksWritten++;

                if (passCount == totalPasses - 1) {
                    this->blocksInBuckets[finalBlocksWritten].emplace_back(rows.size());
                    bufferManager.writeHashPage(this->tableName, finalBlocksWritten++, 0, rows);
                    // this->rowsPerBlockCount[finalBlocksWritten] = rows.size();
                    // bufferManager.writeTablePage(this->tableName, finalBlocksWritten++, rows, rows.size());
                } else {
                    this->rowsPerBlockCount[runPageIndex] = rows.size();
                    bufferManager.writeTablePage(this->tableName, runPageIndex, rows, rows.size());
                    this->blockCount++;
                }

                pagesInRun[(passCount + 1) * zerothPassRunsCount + runsWritten]++;
                rows.clear();
            }
            runsRead += minSize;
            runsWritten++;
        }

        passCount++;
        runsCount = ceil((float)runsCount / (bufferSize - 1));
    }

    if (totalPasses == 0) {
        vector<vector<int>> rowsToWrite;
        vector<vector<int>> pageData;
        for (int i = newBlockCount; i < this->blockCount; i++) {
            pageData = bufferManager.getTablePage(this->tableName, i).data;

            int rowsWritten = 0;

            if (rowsToWrite.size()) {
                if (pageData.size() >= newMaxRowsPerBlock - rowsToWrite.size()) {
                    rowsWritten = newMaxRowsPerBlock - rowsToWrite.size();
                    rowsToWrite.insert(std::end(rowsToWrite), std::begin(pageData), std::begin(pageData) + (newMaxRowsPerBlock - rowsToWrite.size()));
                    this->blocksInBuckets[finalBlocksWritten].emplace_back(rowsToWrite.size());
                    bufferManager.writeHashPage(this->tableName, finalBlocksWritten++, 0, rowsToWrite);
                }
            }

            while (rowsWritten + newMaxRowsPerBlock <= pageData.size()) {
                rowsToWrite = {pageData.begin() + rowsWritten, pageData.begin() + rowsWritten + newMaxRowsPerBlock};
                rowsWritten += newMaxRowsPerBlock;
                this->blocksInBuckets[finalBlocksWritten].emplace_back(rowsToWrite.size());
                bufferManager.writeHashPage(this->tableName, finalBlocksWritten++, 0, rowsToWrite);
            }

            if (rowsWritten < pageData.size())
                rowsToWrite = {pageData.begin() + rowsWritten, pageData.end()};
            else
                rowsToWrite.clear();

            pageData.clear();
        }

        if (rowsToWrite.size()) {
            this->blocksInBuckets[finalBlocksWritten].emplace_back(rowsToWrite.size());
            bufferManager.writeHashPage(this->tableName, finalBlocksWritten++, 0, rowsToWrite);
        }
    }

    for (int i = newBlockCount; i < this->blockCount; i++)
        bufferManager.deleteTableFile(this->tableName, i);
    for (int i = 0; i < originalBlockCount; i++)
        bufferManager.deleteTableFile(this->tableName, i);

    // this->maxRowsPerBlock = newMaxRowsPerBlock;
    this->blockCount = this->blocksInBuckets.size();  //or finalBlocksWritten or newBlockCount
    this->rowsPerBlockCount.clear();

    return;
}

// B+TREE INDEXING
/**
 * @brief Indexes a table using B+-tree
 * 
 * @param columnName
 * 
 * @param fanout
 * 
 * @return 
 */
void Table::bTreeIndex(const string &columnName, int fanout) {
    logger.log("Table::bTreeIndex");
    int colIndex = this->getColumnIndex(columnName);
    this->clearIndex();

    /* This would:
     * clear rowsPerBlockCount
     * and set blocksInBucket
     * 
     * Unlike linearhash, every bucket is initially guaranteed
     * to contain something.
     */
    this->sort(BUFFER_SIZE, columnName, INIT_INDEXED_CAPACITY, 0);
    // TODO: check metadata match

    // TODO: Figure out order and reserve
    this->bTree.reset(fanout, DEFAULT_INDEX_RESERVE);

    // Insert into B+Tree and set bucket ranges
    this->bucketRanges.resize(this->blocksInBuckets.size());
    fill(this->bucketRanges.begin(), this->bucketRanges.end(), make_pair(INT_MAX, INT_MIN));

    Cursor cursor;  // avoid constructor destructor calls
    vector<int> row;
    for (int bucket = this->blocksInBuckets.size() - 1; bucket >= 0; bucket--) {
        cursor = this->getCursor(bucket, 0);
        row = cursor.getNextInBucket();
        while (!row.empty()) {
            this->bucketRanges[bucket].first = min(this->bucketRanges[bucket].first, row[colIndex]);
            this->bucketRanges[bucket].second = max(this->bucketRanges[bucket].second, row[colIndex]);

            // subsequent buckets will overwrite this, that's why going in reverse order of buckets
            this->bTree.insert(row[colIndex], bucket);

            row = cursor.getNextInBucket();
        }
    }

    this->indexed = true;
    this->indexingStrategy = BTREE;
    this->indexedColumn = colIndex;
}