#ifndef __GLOBAL_H
#define __GLOBAL_H

#include"executor.h"
#include "utils.h"

extern float BLOCK_SIZE;
extern uint BLOCK_COUNT;
extern uint PRINT_COUNT;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern TableCatalogue tableCatalogue;
extern MatrixCatalogue matrixCatalogue;
extern BufferManager bufferManager;
extern int MATRIX_PAGE_DIM;

#define MOD(a, m) ((((a) % (m)) + (m)) % (m))

#endif