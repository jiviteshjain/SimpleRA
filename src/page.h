#ifndef __PAGE_H
#define __PAGE_H

#include "logger.h"
/**
 * @brief The Page object is the main memory representation of a physical page
 * (equivalent to a block). The page class and the page.h header file are at the
 * bottom of the dependency tree when compiling files. 
 *<p>
 * Do NOT modify the Page class. If you find that modifications
 * are necessary, you may do so by posting the change you want to make on Moodle
 * or Teams with justification and gaining approval from the TAs. 
 *</p>
 */

#define TABLE_PAGE_NAME(table, index) ("../data/temp/" + (table) + "_T-Page_" + to_string(index))
#define MATRIX_PAGE_NAME(matrix, row, col) ("../data/temp/" + (matrix) + "_M-Page_" + to_string(row) + "_" + to_string(col))
#define HASH_PAGE_NAME(table, bucket, chain) ("../data/temp/" + (table) + "_H-Page_" + to_string(bucket) + "_" + to_string(chain))

class Page {

   protected:
    string tableName;
    // TODO: Remove rowCount and columnCount
    int columnCount;
    int rowCount;
   
   public:
    // TODO: Make these protected
    string pageName = "";
    vector<vector<int>> data;
    vector<int> getRow(int rowIndex);
    Page();
    void writePage();
    
};

class TablePage : public Page {

    string pageIndex;

   public:
    TablePage();
    TablePage(string tableName, int pageIndex);
    TablePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount);
};

class HashPage : public Page {
    int bucket;
    int chainCount;
   public:
    HashPage();
    HashPage(const string& tableName, int bucket, int chainCount);
    HashPage(const string& tableName, int bucket, int chainCount, const vector<vector<int>>& data);
};

class MatrixPage : public Page {

    string matrixName;
    int rowIndex;
    int colIndex;
    vector<vector<int>> matrix;

   public:
    MatrixPage();
    MatrixPage(const string& matrixName, int rowIndex, int colIndex);
    MatrixPage(const string& matrixName, int rowIndex, int colIndex, const vector<vector<int>>& data);
    bool writePage();
    vector<vector<int>> getMatrix();
};

typedef variant<Page, TablePage, HashPage, MatrixPage> Pages;

string getPageName(const Pages& page);

#endif