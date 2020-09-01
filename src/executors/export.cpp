#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT;
    parsedQuery.exportRelationOrMatrixName = tokenizedQuery[1];
    return true;
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    //Table should exist
    if (tableCatalogue.isTable(parsedQuery.exportRelationOrMatrixName) || matrixCatalogue.isMatrix(parsedQuery.exportRelationOrMatrixName))
        return true;
    cout << "SEMANTIC ERROR: No such relation or matrix exists" << endl;
    return false;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    if (tableCatalogue.isTable(parsedQuery.exportRelationOrMatrixName))
    {
        Table* table = tableCatalogue.getTable(parsedQuery.exportRelationOrMatrixName);
        table->makePermanent();
    }
    else if (matrixCatalogue.isMatrix(parsedQuery.exportRelationOrMatrixName))
    {
        Matrix* matrix = matrixCatalogue.getMatrix(parsedQuery.exportRelationOrMatrixName);
        matrix->makePermanent();
    }
    return;
}