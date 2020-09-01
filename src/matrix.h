#ifndef __MATRIX_H
#define __MATRIX_H

#include "bufferManager.h"

/**
 * @brief The Table class holds all information related to a loaded table. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a table object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT). 
 *
 */
class Matrix {
   private:
    void initCalc(const string& line);
    vector<int> readLine(const string& line, int colNo);
    void writeLine(const vector<int>& line, ofstream& fout);

   public:
    string sourceFileName = "";
    string matrixName = "";
    long long int dimension; // gauranteed to be square
    long long int blockCount; // along one dimension, actually blockCount^2 blocks

    bool blockify();
    Matrix();
    Matrix(const string& tableName);
    
    bool load();
    void makePermanent();
    void unload();
    void transpose();
};

#endif