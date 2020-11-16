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

    Table *table = tableCatalogue.getTable(parsedQuery.alterTableRelationName);

    if (parsedQuery.alterTableOperationName == "ADD")
        if (table->isColumn(parsedQuery.alterTableColumnName))
        {
            cout << "SEMANTIC ERROR: Column already exists in relation";
            return false;
        }

    if (parsedQuery.alterTableOperationName == "DELETE")
        if (!table->isColumn(parsedQuery.alterTableColumnName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation";
            return false;
        }
    return true;
}

void executeALTERTABLE()
{
    logger.log("executeALTERTABLE");
    Table *table = tableCatalogue.getTable(parsedQuery.alterTableRelationName);

    vector<string> columns = table->columns;
    int columnIndex;
    if (parsedQuery.alterTableOperationName == "ADD")
        columns.push_back(parsedQuery.alterTableColumnName);
    else if (parsedQuery.alterTableOperationName == "DELETE")
    {
        columnIndex = table->getColumnIndex(parsedQuery.alterTableColumnName);
        columns.erase(columns.begin() + columnIndex);
        if (columns.size() == 0)
        {
            tableCatalogue.deleteTable(table->tableName);
            return;
        }
    }

    Table *newTable = new Table(parsedQuery.alterTableRelationName + "_altertemp", columns);
    tableCatalogue.insertTable(newTable);
    newTable->blockCount = 0;

    if (!table->indexed || (table->indexingStrategy == HASH && table->indexedColumn == columnIndex))
    {
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
        vector<vector<int>> rows;
        while (!row.empty())
        {
            if (parsedQuery.alterTableOperationName == "ADD")
                row.push_back(0);
            else if (parsedQuery.alterTableOperationName == "DELETE")
                row.erase(row.begin() + columnIndex);

            newTable->updateStatistics(row);

            rows.push_back(row);
            if (rows.size() == newTable->maxRowsPerBlock)
            {
                newTable->rowsPerBlockCount.emplace_back(rows.size());
                bufferManager.writeTablePage(newTable->tableName, newTable->blockCount, rows, rows.size());
                newTable->blockCount++;
                rows.clear();
            }

            if (!table->indexed)
                row = cursor.getNext();
            else if (table->indexingStrategy == HASH)
                row = cursor.getNextInAllBuckets();
        }

        if (rows.size())
        {
            newTable->rowsPerBlockCount.emplace_back(rows.size());
            bufferManager.writeTablePage(newTable->tableName, newTable->blockCount, rows, rows.size());
            newTable->blockCount++;
            rows.clear();
        }
    }

    else if (table->indexingStrategy == HASH)
    {
        newTable->indexed = true;
        newTable->indexedColumn = table->indexedColumn;
        newTable->indexingStrategy = HASH;
        newTable->initialBucketCount = table->initialBucketCount;
        newTable->M = table->M;
        newTable->N = table->N;
        newTable->blocksInBuckets = vector<vector<int>>(table->blocksInBuckets.size());
        for (int bucket = 0; bucket < table->blocksInBuckets.size(); bucket++)
        {
            if (table->blocksInBuckets[bucket].size())
            {
                Cursor cursor = table->getCursor(bucket, 0);
                vector<int> row = cursor.getNextInBucket();
                vector<vector<int>> rows;
                int chainCount = 0;

                while (!row.empty())
                {
                    if (parsedQuery.alterTableOperationName == "ADD")
                        row.push_back(0);
                    else if (parsedQuery.alterTableOperationName == "DELETE")
                        row.erase(row.begin() + columnIndex);

                    newTable->updateStatistics(row);
                    rows.push_back(row);
                    if (rows.size() == newTable->maxRowsPerBlock)
                    {
                        newTable->blocksInBuckets[bucket].emplace_back(rows.size());
                        bufferManager.writeHashPage(newTable->tableName, bucket, chainCount++, rows);
                        newTable->blockCount++;
                        rows.clear();
                    }

                    row = cursor.getNextInBucket();
                }

                if (rows.size())
                {
                    newTable->blocksInBuckets[bucket].emplace_back(rows.size());
                    bufferManager.writeHashPage(newTable->tableName, bucket, chainCount++, rows);
                    newTable->blockCount++;
                    rows.clear();
                }
            }
        }
    }

    if (newTable->rowCount)
    {
        newTable->sourceFileName = table->sourceFileName;
        string tableName = table->tableName;
        tableCatalogue.deleteTable(table->tableName);
        vector<vector<int>> rows;
        if (!newTable->indexed || (newTable->indexingStrategy == HASH && newTable->indexedColumn == columnIndex))
        {
            for (int i = 0; i < newTable->blockCount; i++)
            {
                rows = bufferManager.getTablePage(newTable->tableName, i).data;
                bufferManager.deleteTableFile(newTable->tableName, i);
                // rows.resize(newTable->rowsPerBlockCount[i]);
                bufferManager.writeTablePage(tableName, i, rows, rows.size());
            }
        }
        else if (newTable->indexingStrategy == HASH)
        {
            for (int i = 0; i < newTable->blocksInBuckets.size(); i++)
                for (int j = 0; j < newTable->blocksInBuckets[i].size(); j++)
                {
                    rows = bufferManager.getHashPage(newTable->tableName, i, j).data;
                    bufferManager.deleteHashFile(newTable->tableName, i, j);
                    bufferManager.writeHashPage(tableName, i, j, rows);
                }
        }
        
        tableCatalogue.replaceTableName(newTable->tableName, tableName);
        newTable->tableName = tableName;
    }
    else
    {
        cout << "Empty Table" << endl;
        newTable->unload();
        delete newTable;
    }
    return;
}
