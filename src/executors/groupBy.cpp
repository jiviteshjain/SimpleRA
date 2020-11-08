#include "global.h"
/**
 * @brief 
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
 */
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[3] != "BY" || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "RETURN" || tokenizedQuery[8].length() <= 5 || tokenizedQuery[8][tokenizedQuery[8].length() - 1] != ')' || tokenizedQuery[8][3] != '(' || (tokenizedQuery[8].substr(0, 3) != "MAX" && tokenizedQuery[8].substr(0, 3) != "MIN" && tokenizedQuery[8].substr(0, 3) != "SUM" && tokenizedQuery[8].substr(0, 3) != "AVG"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupByResultRelationName = tokenizedQuery[0];
    parsedQuery.groupByGroupingAttributeName = tokenizedQuery[4];
    parsedQuery.groupByRelationName = tokenizedQuery[6];
    parsedQuery.groupByOperatorName = tokenizedQuery[8].substr(0, 3);
    parsedQuery.groupByAttributeName = tokenizedQuery[8].substr(4, tokenizedQuery[8].length() - 5);

    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupByResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }
    if (!tableCatalogue.isTable(parsedQuery.groupByRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table *table = tableCatalogue.getTable(parsedQuery.groupByRelationName);
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByGroupingAttributeName, parsedQuery.groupByRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.groupByAttributeName, parsedQuery.groupByRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    return true;
}

void executeGROUPBY()
{
    logger.log("executeGROUPBY");
    Table *table = tableCatalogue.getTable(parsedQuery.groupByRelationName);
    Table* resultantTable = new Table(parsedQuery.groupByResultRelationName, {parsedQuery.groupByGroupingAttributeName, parsedQuery.groupByOperatorName + parsedQuery.groupByAttributeName});

    int firstColumnIndex = table->getColumnIndex(parsedQuery.groupByGroupingAttributeName);
    int secondColumnIndex = table->getColumnIndex(parsedQuery.groupByAttributeName);

    map<int, int> result;
    map<int, int> count;

    vector<int> row;
    Cursor cursor;

    if (!table->indexed)
    {
        cursor = table->getCursor();
        row = cursor.getNext();
    }
    else if (table->indexingStrategy == HASH)
    {
        cursor = table->getCursor(0, 0);
        row = cursor.getNextInAllBuckets();
    }

    while (!row.empty())
    {
        if (parsedQuery.groupByOperatorName == "MAX")
        {
            if (result.find(row[firstColumnIndex]) == result.end())
                result[row[firstColumnIndex]] = row[secondColumnIndex];
            result[row[firstColumnIndex]] = max(result[row[firstColumnIndex]], row[secondColumnIndex]);
        }
        else if (parsedQuery.groupByOperatorName == "MIN")
        {
            if (result.find(row[firstColumnIndex]) == result.end())
                result[row[firstColumnIndex]] = row[secondColumnIndex];
            result[row[firstColumnIndex]] = min(result[row[firstColumnIndex]], row[secondColumnIndex]);
        }
        else
        {
            result[row[firstColumnIndex]] += row[secondColumnIndex];
            if (parsedQuery.groupByOperatorName == "AVG")
                count[row[firstColumnIndex]]++;
        }

        if (!table->indexed)
            row = cursor.getNext();
        else if (table->indexingStrategy == HASH)
            row = cursor.getNextInAllBuckets();
    }

    vector<vector<int>> rows;
    for (auto it: result)
    {
        if (parsedQuery.groupByOperatorName == "AVG")
        {
            resultantTable->updateStatistics({it.first, it.second/count[it.first]});
            rows.push_back({it.first, it.second/count[it.first]});
        }
        else
        {
            resultantTable->updateStatistics({it.first, it.second});
            rows.push_back({it.first, it.second});
        }

        if (rows.size() == resultantTable->maxRowsPerBlock)
        {
            resultantTable->rowsPerBlockCount.emplace_back(rows.size());
            bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
            resultantTable->blockCount++;
            rows.clear();
        }
    }

    if (rows.size())
    {
        resultantTable->rowsPerBlockCount.emplace_back(rows.size());
        bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
        resultantTable->blockCount++;
        rows.clear();
    }

    if (resultantTable->rowCount)
        tableCatalogue.insertTable(resultantTable);
    else{
        cout<<"Empty Table"<<endl;
        resultantTable->unload();
        delete resultantTable;
    }
}