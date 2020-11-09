#include "global.h"
/**
 * @brief 
 * SYNTAX: ALTER TABLE <table_name> ADD|DELETE COLUMN <column_name>
 */
bool syntacticParseALTERTABLE()
{
    logger.log("syntacticParseALTERTABLE");
    if (tokenizedQuery.size() != 6 || (tokenizedQuery[3] != "ADD" && tokenizedQuery[3] != "DELETE") || tokenizedQuery[4] != "COLUMN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = ALTERTABLE;
    parsedQuery.alterTableRelationName = tokenizedQuery[2];
    parsedQuery.alterTableOperationName = tokenizedQuery[3];
    parsedQuery.alterTableColumnName = tokenizedQuery[5];

    return true;
}

bool semanticParseALTERTABLE()
{
    logger.log("semanticParseALTERTABLE");
    if (!tableCatalogue.isTable(parsedQuery.alterTableRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (parsedQuery.alterTableOperationName == "ADD")
        if (tableCatalogue.getTable(parsedQuery.alterTableRelationName)->isColumn(parsedQuery.alterTableColumnName))
        {
            cout << "SEMANTIC ERROR: Column already exists in relation";
            return false;
        }

    if (parsedQuery.alterTableOperationName == "DELETE")
        if (!tableCatalogue.getTable(parsedQuery.alterTableRelationName)->isColumn(parsedQuery.alterTableColumnName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation";
            return false;
        }
    return true;
}

void executeALTERTABLE()
{
    logger.log("executeALTERTABLE");

    // Table *table = new Table(parsedQuery.loadRelationName);
    // if (table->load())
    // {
    //     tableCatalogue.insertTable(table);
    //     cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
    // }
    return;
}