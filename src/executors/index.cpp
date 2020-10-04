#include "global.h"
/**
 * @brief 
 * SYNTAX: INDEX ON column_name FROM relation_name USING indexing_strategy
 * indexing_strategy: ASC | DESC | NOTHING
 */
bool syntacticParseINDEX()
{
    logger.log("syntacticParseINDEX");
    if ((tokenizedQuery.size() != 7 && tokenizedQuery.size() != 9) || tokenizedQuery[1] != "ON" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "USING")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = INDEX;
    parsedQuery.indexColumnName = tokenizedQuery[2];
    parsedQuery.indexRelationName = tokenizedQuery[4];
    string indexingStrategy = tokenizedQuery[6];

    if (indexingStrategy == "BTREE")
    {
        parsedQuery.indexingStrategy = BTREE;
        if (tokenizedQuery[7] != "FANOUT")
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    else if (indexingStrategy == "HASH")
    {
        parsedQuery.indexingStrategy = HASH;
        if (tokenizedQuery[7] != "BUCKETS")
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    else if (indexingStrategy == "NOTHING")
        parsedQuery.indexingStrategy = NOTHING;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseINDEX()
{
    logger.log("semanticParseINDEX");
    if (!tableCatalogue.isTable(parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    if (!tableCatalogue.isColumnFromTable(parsedQuery.indexColumnName, parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    Table* table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    if(table->indexed && parsedQuery.indexingStrategy != NOTHING){
        cout << "SEMANTIC ERROR: Table already indexed" << endl;
        return false;
    }
    return true;
}

void executeINDEX()
{
    Table* table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    table->indexingStrategy = parsedQuery.indexingStrategy;
    cout<<"Table = "<<table->tableName<<endl<<"Strategy = "<<table->indexingStrategy<<endl;
    if (table->indexingStrategy == NOTHING)
    {
        table->indexed = false;
        table->indexedColumn = "";
        // clearIndex();
    }
    else
    {
        table->indexed=true;
        table->indexedColumn = parsedQuery.indexColumnName;
        if (table->indexingStrategy == HASH)
            table->linearHash(parsedQuery.indexColumnName, stoi(tokenizedQuery[8]));
        else if (table->indexingStrategy == BTREE)
            ;
            // bPlusTree(atoi(tokenizedQuery[8].c_str()));
    }
    cout<<"Column = "<<table->indexedColumn<<endl<<"Indexed = "<<table->indexed<<endl;
    logger.log("executeINDEX");
    return;
}