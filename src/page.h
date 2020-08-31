#ifndef __PAGE_H
#define __PAGE_H

#include"logger.h"
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

enum DataType {
    TABLE,
    MATRIX
};

class Page{

    string tableName;
    string pageIndex;
    int columnCount;
    int rowCount;
    vector<vector<int>> rows;

    string matrixName;
    int rowIndex;
    int colIndex;
    vector<vector<int>> matrix;

    public:

    DataType type = TABLE;

    string pageName = "";
    Page();
    Page(string tableName, int pageIndex);
    Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount);
    vector<int> getRow(int rowIndex);
    void writePage();

    Page(const string& matrixName, int rowIndex, int colIndex);
    Page(const string& matrixName, int rowIndex, int colIndex, const vector<vector<int>>& data);
    vector<vector<int>> getMatrix();
    bool writeMatrixPage();
};

#endif