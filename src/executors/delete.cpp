#include "global.h"
/**
 * @brief 
 * SYNTAX: DELETE FROM table_name VALUES <value1>[,<value2>]*
 */
bool syntacticParseDELETE()
{
    logger.log("syntacticParseDELETE");
    if (tokenizedQuery.size() < 5 || tokenizedQuery[1] != "FROM" || tokenizedQuery[3] != "VALUES")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = DELETE;
    parsedQuery.deleteRelationName = tokenizedQuery[2];

    return true;
}

bool semanticParseDELETE()
{
    logger.log("semanticParseDELETE");

    if (!tableCatalogue.isTable(parsedQuery.deleteRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table table = *tableCatalogue.getTable(parsedQuery.deleteRelationName);
    if (tokenizedQuery.size() - 4 != table.columnCount)
    {
        cout << "SEMANTIC ERROR: Incorrect number of values entered" << endl;
        return false;
    }

    parsedQuery.deleteRow.clear();
    for (int i = tokenizedQuery.size() - table.columnCount; i < tokenizedQuery.size(); i++)
        parsedQuery.deleteRow.push_back(stoi(tokenizedQuery[i]));
    
    return true;
}

void executeDELETE()
{
    logger.log("executeDELETE");
    Table table = *tableCatalogue.getTable(parsedQuery.deleteRelationName);
    cout<<table.remove(parsedQuery.deleteRow)<<endl;
}