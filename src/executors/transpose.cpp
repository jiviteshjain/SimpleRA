#include "global.h"
/**
 * @brief 
 * SYNTAX: TRANSPOSE matrix_name
 */
bool syntacticParseTRANSPOSE()
{
    logger.log("syntacticParseTRANSPOSE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = TRANSPOSE;
    parsedQuery.transposeMatrixName = tokenizedQuery[1];
    return true;
}

bool semanticParseTRANSPOSE()
{
    logger.log("semanticParseTRANSPOSE");
    if (!matrixCatalogue.isMatrix(parsedQuery.transposeMatrixName))
    {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;

    if (checkFileType(parsedQuery.transposeMatrixName) != MATRIX)
    {
        cout << "SEMANTIC ERROR: The file isn't a matrix" << endl;
        return false;
    }

    return true;
}

void executeTRANSPOSE()
{
    logger.log("executeTRANSPOSE");
    Matrix *matrix = matrixCatalogue.getMatrix(parsedQuery.transposeMatrixName);
    matrix->transpose();
    return;
}