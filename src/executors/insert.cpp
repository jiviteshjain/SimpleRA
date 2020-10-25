#include "global.h"
/**
 * @brief 
 * SYNTAX: INSERT INTO table_name VALUES <value1>[,<value2>]*
 */
bool syntacticParseINSERT()
{
    logger.log("syntacticParseINSERT");
    if (tokenizedQuery.size() < 5 || tokenizedQuery[1] != "INTO" || tokenizedQuery[3] != "VALUES")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = INSERT;
    parsedQuery.insertRelationName = tokenizedQuery[2];

    return true;
}

bool semanticParseINSERT()
{
    logger.log("semanticParseINSERT");

    if (!tableCatalogue.isTable(parsedQuery.insertRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table* table = tableCatalogue.getTable(parsedQuery.insertRelationName);
    if (tokenizedQuery.size() - 4 != table->columnCount)
    {
        cout << "SEMANTIC ERROR: Incorrect number of values entered" << endl;
        return false;
    }

    for (int i = tokenizedQuery.size() - table->columnCount; i < tokenizedQuery.size(); i++)
        parsedQuery.insertRow.push_back(stoi(tokenizedQuery[i]));

    return true;
}

void executeINSERT()
{
    logger.log("executeINSERT");
    Table* table = tableCatalogue.getTable(parsedQuery.insertRelationName);
    table->insert(parsedQuery.insertRow);
}