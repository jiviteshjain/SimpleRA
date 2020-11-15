#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page() {
    logger.log("Page::Page");

    this->pageName = "";
    this->tableName = "";
    this->rowCount = 0;
    this->columnCount = 0;
    this->data.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName 
 * @param pageIndex 
 */
TablePage::TablePage(string tableName, int pageIndex) {
    logger.log("TablePage::TablePage");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->pageName = TABLE_PAGE_NAME(tableName, pageIndex);
    Table *table = tableCatalogue.getTable(tableName);
    this->columnCount = table->columnCount;
    uint maxRowCount = table->maxRowsPerBlock;
    this->rowCount = table->rowsPerBlockCount[pageIndex];
    vector<int> row(columnCount, 0);
    this->data.assign(this->rowCount, row);
    ifstream fin(pageName, ios::in);
    int number;
    for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
        for (int columnCounter = 0; columnCounter < columnCount; columnCounter++) {
            fin >> number;
            this->data[rowCounter][columnCounter] = number;
        }
    }
    fin.close();
}

/**
 * @brief Get row from page indexed by rowIndex
 * 
 * @param rowIndex 
 * @return vector<int> 
 */
vector<int> Page::getRow(int rowIndex) {
    logger.log("Page::getRow");
    vector<int> result;
    result.clear();
    if (rowIndex >= this->rowCount)
        return result;
    return this->data[rowIndex];
}

TablePage::TablePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount) {
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rowCount = rowCount;
    rows.resize(this->rowCount);
    this->data = rows;
    this->columnCount = rows[0].size();    
    this->pageName = TABLE_PAGE_NAME(this->tableName, pageIndex);
}

/**
 * @brief writes current page contents to file.
 * 
 */
void Page::writePage() {
    logger.log("Page::writePage");
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++) {
            if (columnCounter != 0)
                fout << " ";
            fout << this->data[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}

MatrixPage::MatrixPage(const string& matrixName, int rowIndex, int colIndex) {
    logger.log("MatrixPage::MatrixPage");

    this->matrixName = matrixName;
    this->rowIndex = rowIndex;
    this->colIndex = colIndex;
    this->pageName = MATRIX_PAGE_NAME(this->matrixName, this->rowIndex, this->colIndex);

    this->matrix.resize(MATRIX_PAGE_DIM);
    fill(this->matrix.begin(), this->matrix.end(), vector<int>(MATRIX_PAGE_DIM, -1));

    ifstream fin(this->pageName, ios::in);
    for (int i = 0; i < MATRIX_PAGE_DIM; i++) {
        for (int j = 0; j < MATRIX_PAGE_DIM; j++) {
            int temp;
            fin >> temp;
            this->matrix[i][j] = temp;  // -1's are also filled
        }
    }
}

MatrixPage::MatrixPage(const string& matrixName, int rowIndex, int colIndex, const vector<vector<int>>& data) {
    logger.log("MatrixPage::MatrixPage");

    this->matrixName = matrixName;
    this->rowIndex = rowIndex;
    this->colIndex = colIndex;
    this->pageName = MATRIX_PAGE_NAME(this->matrixName, this->rowIndex, this->colIndex);

    this->matrix = data;
}

vector<vector<int>> MatrixPage::getMatrix() {
    logger.log("MatrixPage::getMatrix");

    return this->matrix;
}

bool MatrixPage::writePage() {
    logger.log("MatrixPage::writePage");

    ofstream fout(this->pageName, ios::trunc);
    for (int i = 0; i < MATRIX_PAGE_DIM; i++) {
        for (int j = 0; j < MATRIX_PAGE_DIM; j++) {
            fout << this->matrix[i][j];
            if (j != MATRIX_PAGE_DIM - 1) {
                fout << " ";
            }
        }
        fout << endl;
    }
    fout.close();
    return true;
}

HashPage::HashPage(const string& tableName, int bucket, int chainCount) {
    logger.log("HashPage::HashPage");
    this->tableName = tableName;
    this->bucket = bucket;
    this->chainCount = chainCount;
    this->pageName = HASH_PAGE_NAME(this->tableName, this->bucket, this->chainCount);
    Table *table = tableCatalogue.getTable(tableName);
    this->columnCount = table->columnCount;
    uint maxRowCount = table->maxRowsPerBlock;
    vector<int> row(columnCount, 0);
    this->rowCount = table->blocksInBuckets[bucket][chainCount];
    this->data.assign(this->rowCount, row);

    ifstream fin(pageName, ios::in);
    int number;
    for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++) {
            fin >> number;
            this->data[rowCounter][columnCounter] = number;
        }
    }
    fin.close();
}

HashPage::HashPage(const string& tableName, int bucket, int chainCount, const vector<vector<int>>& rows) {
    logger.log("HashPage::HashPage");
    this->tableName = tableName;
    this->bucket = bucket;
    this->chainCount = chainCount;
    this->data = rows;
    this->rowCount = rows.size();
    this->columnCount = rows[0].size();
    this->pageName = HASH_PAGE_NAME(this->tableName, this->bucket, this->chainCount);
}

// VARIANT VISITORS

string getPageName(const Pages& page) {
    return visit([](auto&& arg){return arg.pageName;}, page);
}

void writePage(Pages& page) { // cannot gaurantee const on this, because calls a member function
    visit([](auto&& arg){arg.writePage();}, page);
}