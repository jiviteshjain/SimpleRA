#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- SELECT column_name bin_op [column_name | int_literal] FROM relation_name
 */
bool syntacticParseSELECTION()
{
    logger.log("syntacticParseSELECTION");
    if (tokenizedQuery.size() != 8 || tokenizedQuery[6] != "FROM")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SELECTION;
    parsedQuery.selectionResultRelationName = tokenizedQuery[0];
    parsedQuery.selectionFirstColumnName = tokenizedQuery[3];
    parsedQuery.selectionRelationName = tokenizedQuery[7];

    string binaryOperator = tokenizedQuery[4];
    if (binaryOperator == "<")
        parsedQuery.selectionBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.selectionBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.selectionBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.selectionBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.selectionBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.selectionBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    regex numeric("[-]?[0-9]+");
    string secondArgument = tokenizedQuery[5];
    if (regex_match(secondArgument, numeric))
    {
        parsedQuery.selectType = INT_LITERAL;
        parsedQuery.selectionIntLiteral = stoi(secondArgument);
    }
    else
    {
        parsedQuery.selectType = COLUMN;
        parsedQuery.selectionSecondColumnName = secondArgument;
    }
    return true;
}

bool semanticParseSELECTION()
{
    logger.log("semanticParseSELECTION");

    if (tableCatalogue.isTable(parsedQuery.selectionResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.selectionRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.selectionFirstColumnName, parsedQuery.selectionRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (parsedQuery.selectType == COLUMN)
    {
        if (!tableCatalogue.isColumnFromTable(parsedQuery.selectionSecondColumnName, parsedQuery.selectionRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }
    }
    return true;
}

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator)
{
    switch (binaryOperator)
    {
    case LESS_THAN:
        return (value1 < value2);
    case GREATER_THAN:
        return (value1 > value2);
    case LEQ:
        return (value1 <= value2);
    case GEQ:
        return (value1 >= value2);
    case EQUAL:
        return (value1 == value2);
    case NOT_EQUAL:
        return (value1 != value2);
    default:
        return false;
    }
}
/**
 * @brief Checks the bucket for rows that satisfy the condition
 * 
 * @param table
 * 
 * @param resultantTable
 * 
 * @param bucket
 * 
 * @return 
 */
void retrieveResult(Table *table, Table *resultantTable, int bucket)
{
    Cursor cursor = table->getCursor(bucket, 0);
    vector<int> row = cursor.getNextInBucket();
    vector<vector<int>> rows;

    while (!row.empty()) //loop to fill last page
    {
        if (!resultantTable->rowsPerBlockCount.size() || resultantTable->maxRowsPerBlock - resultantTable->rowsPerBlockCount[resultantTable->blockCount - 1] == 0) //break if resultantTable is empty
            break;
        
        int value1 = row[table->getColumnIndex(parsedQuery.selectionFirstColumnName)];
        int value2 = parsedQuery.selectionIntLiteral;
        if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
        {
            rows.push_back(row);
            resultantTable->updateStatistics(row);
        }
        if (rows.size() == resultantTable->maxRowsPerBlock - resultantTable->rowsPerBlockCount[resultantTable->blockCount - 1])
        {
            vector<vector<int>> rowsInLastPage = bufferManager.getTablePage(resultantTable->tableName, resultantTable->blockCount - 1).data;
            // rowsInLastPage.resize(resultantTable->rowsPerBlockCount[resultantTable->blockCount - 1]);
            rowsInLastPage.insert(std::end(rowsInLastPage), std::begin(rows), std::end(rows));
            bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount - 1, rowsInLastPage, rowsInLastPage.size());
            resultantTable->rowsPerBlockCount[resultantTable->blockCount - 1] = resultantTable->maxRowsPerBlock;
            rows.clear();
            row = cursor.getNextInBucket();
            break;
        }
        row = cursor.getNextInBucket();
    }

    while (!row.empty())
    {
        int value1 = row[table->getColumnIndex(parsedQuery.selectionFirstColumnName)];
        int value2 = parsedQuery.selectionIntLiteral;
        if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
        {
            rows.push_back(row);
            resultantTable->updateStatistics(row);
        }
        if (rows.size() == resultantTable->maxRowsPerBlock)
        {
            resultantTable->rowsPerBlockCount.emplace_back(rows.size());
            bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
            resultantTable->blockCount++;
            rows.clear();
        }
        row = cursor.getNextInBucket();
    }

    if (rows.size())
    {
        if (resultantTable->rowsPerBlockCount.size() && rows.size() <= resultantTable->maxRowsPerBlock - resultantTable->rowsPerBlockCount[resultantTable->blockCount - 1])
        {
            vector<vector<int>> rowsInLastPage = bufferManager.getTablePage(resultantTable->tableName, resultantTable->blockCount - 1).data;
            // rowsInLastPage.resize(resultantTable->rowsPerBlockCount[resultantTable->blockCount - 1]);
            rowsInLastPage.insert(std::end(rowsInLastPage), std::begin(rows), std::end(rows));
            bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount - 1, rowsInLastPage, rowsInLastPage.size());
            resultantTable->rowsPerBlockCount[resultantTable->blockCount - 1] = rowsInLastPage.size();
            rows.clear();
        }
        else if (rows.size() < resultantTable->maxRowsPerBlock)
        {
            resultantTable->rowsPerBlockCount.emplace_back(rows.size());
            bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
            resultantTable->blockCount++;
            rows.clear();
        }
    }
}

/**
 * @brief Saves the final result to tableCatalogue
 * 
 * @param resultantTable
 * 
 * @return 
 */
void saveResult(Table *resultantTable)
{
    if (resultantTable->rowCount)
        tableCatalogue.insertTable(resultantTable);
    else{
        cout<<"Empty Table"<<endl;
        resultantTable->unload();
        delete resultantTable;
    }
}

void executeSELECTION()
{

    logger.log("executeSELECTION");
    Table *table = tableCatalogue.getTable(parsedQuery.selectionRelationName);
    Table *resultantTable = new Table(parsedQuery.selectionResultRelationName, table->columns);
    int firstColumnIndex = table->getColumnIndex(parsedQuery.selectionFirstColumnName);
    if (!table->indexed)
    {
        Cursor cursor = table->getCursor();
        vector<int> row = cursor.getNext();
        vector<vector<int>> rows;
        int secondColumnIndex;
        if (parsedQuery.selectType == COLUMN)
            secondColumnIndex = table->getColumnIndex(parsedQuery.selectionSecondColumnName);
        while (!row.empty())
        {
            int value1 = row[firstColumnIndex];
            int value2;
            if (parsedQuery.selectType == INT_LITERAL)
                value2 = parsedQuery.selectionIntLiteral;
            else
                value2 = row[secondColumnIndex];
            if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
            {
                rows.push_back(row);
                resultantTable->updateStatistics(row);
            }
            if (rows.size() == resultantTable->maxRowsPerBlock)
            {
                resultantTable->rowsPerBlockCount.emplace_back(rows.size());
                bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
                resultantTable->blockCount++;
                rows.clear();
            }

            row = cursor.getNext();
        }

        if (rows.size())
        {
            resultantTable->rowsPerBlockCount.emplace_back(rows.size());
            bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
            resultantTable->blockCount++;
            rows.clear();
        }
        saveResult(resultantTable);
        return;
    }
    else if (table->indexingStrategy == HASH)
    {
        if ((table->indexed && table->getIndexedColumn() != parsedQuery.selectionFirstColumnName) || (parsedQuery.selectType == COLUMN) || (parsedQuery.selectionBinaryOperator == NOT_EQUAL))
        {
            Cursor cursor = table->getCursor(0, 0);
            vector<int> row = cursor.getNextInAllBuckets();
            vector<vector<int>> rows;
            int secondColumnIndex;
            if (parsedQuery.selectType == COLUMN)
                secondColumnIndex = table->getColumnIndex(parsedQuery.selectionSecondColumnName);
            while (!row.empty())
            {
                int value1 = row[firstColumnIndex];
                int value2;
                if (parsedQuery.selectType == INT_LITERAL)
                    value2 = parsedQuery.selectionIntLiteral;
                else
                    value2 = row[secondColumnIndex];
                if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
                {
                    rows.push_back(row);
                    resultantTable->updateStatistics(row);
                }
                if (rows.size() == resultantTable->maxRowsPerBlock)
                {
                    resultantTable->rowsPerBlockCount.emplace_back(rows.size());
                    bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
                    resultantTable->blockCount++;
                    rows.clear();
                }
                row = cursor.getNextInAllBuckets();
            }

            if (rows.size())
            {
                resultantTable->rowsPerBlockCount.emplace_back(rows.size());
                bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
                resultantTable->blockCount++;
                rows.clear();
            }
            saveResult(resultantTable);
        }
        else if (parsedQuery.selectionBinaryOperator == EQUAL)
        {
            retrieveResult(table, resultantTable, table->hash(parsedQuery.selectionIntLiteral));
            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == LESS_THAN)
        {
            set<int> bucketsVisited;
            for (int i = table->smallestInColumns[firstColumnIndex]; i < parsedQuery.selectionIntLiteral && bucketsVisited.size() != table->blocksInBuckets.size(); i++)
            {
                int bucket = table->hash(i);
                if (bucketsVisited.find(bucket) == bucketsVisited.end())
                {
                    retrieveResult(table, resultantTable, bucket);
                    bucketsVisited.insert(bucket);
                }
            }    
    
            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == LEQ)
        {
            set<int> bucketsVisited;
            for (int i = table->smallestInColumns[firstColumnIndex]; i <= parsedQuery.selectionIntLiteral && bucketsVisited.size() != table->blocksInBuckets.size(); i++)
            {
                int bucket = table->hash(i);
                if (bucketsVisited.find(bucket) == bucketsVisited.end())
                {
                    retrieveResult(table, resultantTable, bucket);
                    bucketsVisited.insert(bucket);
                }
            }    

            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == GREATER_THAN)
        {
            set<int> bucketsVisited;
            for (int i = parsedQuery.selectionIntLiteral + 1; i < table->largestInColumns[firstColumnIndex] && bucketsVisited.size() != table->blocksInBuckets.size(); i++)
            {
                int bucket = table->hash(i);
                if (bucketsVisited.find(bucket) == bucketsVisited.end())
                {
                    retrieveResult(table, resultantTable, bucket);
                    bucketsVisited.insert(bucket);
                }
            }    

            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == GEQ)
        {
            set<int> bucketsVisited;
            for (int i = parsedQuery.selectionIntLiteral + 1; i < table->largestInColumns[firstColumnIndex] && bucketsVisited.size() != table->blocksInBuckets.size(); i++)
            {
                int bucket = table->hash(i);
                if (bucketsVisited.find(bucket) == bucketsVisited.end())
                {
                    retrieveResult(table, resultantTable, bucket);
                    bucketsVisited.insert(bucket);
                }
            }    

            saveResult(resultantTable);
            return;
        }
    }
    else if (table->indexingStrategy == BTREE)
    {
        if ((table->indexed && table->getIndexedColumn() != parsedQuery.selectionFirstColumnName) || (parsedQuery.selectType == COLUMN) || (parsedQuery.selectionBinaryOperator == NOT_EQUAL))
        {
            Cursor cursor = table->getCursor(0, 0);
            vector<int> row = cursor.getNextInAllBuckets();
            vector<vector<int>> rows;
            int secondColumnIndex;
            if (parsedQuery.selectType == COLUMN)
                secondColumnIndex = table->getColumnIndex(parsedQuery.selectionSecondColumnName);
            while (!row.empty())
            {
                int value1 = row[firstColumnIndex];
                int value2;
                if (parsedQuery.selectType == INT_LITERAL)
                    value2 = parsedQuery.selectionIntLiteral;
                else
                    value2 = row[secondColumnIndex];
                if (evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator))
                {
                    rows.push_back(row);
                    resultantTable->updateStatistics(row);
                }
                if (rows.size() == resultantTable->maxRowsPerBlock)
                {
                    resultantTable->rowsPerBlockCount.emplace_back(rows.size());
                    bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
                    resultantTable->blockCount++;
                    rows.clear();
                }
                row = cursor.getNextInAllBuckets();
            }

            if (rows.size())
            {
                resultantTable->rowsPerBlockCount.emplace_back(rows.size());
                bufferManager.writeTablePage(resultantTable->tableName, resultantTable->blockCount, rows, rows.size());
                resultantTable->blockCount++;
                rows.clear();
            }
            saveResult(resultantTable);
        }
        else if (parsedQuery.selectionBinaryOperator == EQUAL)
        {
            auto record = table->bTree.find(parsedQuery.selectionIntLiteral, nullptr);
            if (record == nullptr) 
            {
                saveResult(resultantTable);
                return;
            }
            
            int bucket_i = record->val();
            int bucket_f = bucket_i;
            while (bucket_f < table->blocksInBuckets.size())
            {
                if (table->bucketRanges[bucket_f].first <= parsedQuery.selectionIntLiteral)
                    bucket_f++;
                else
                    break;
            }

            for (int bucket = bucket_f - 1; bucket >= bucket_i; bucket--) 
                retrieveResult(table, resultantTable, bucket);
            
            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == LESS_THAN)
        {
            for (int i = 0; i < table->blocksInBuckets.size(); i++)
            {
                if (table->blocksInBuckets[i].size())
                {
                    if (table->bucketRanges[i].first < parsedQuery.selectionIntLiteral)
                        retrieveResult(table, resultantTable, i);
                    else
                        break;
                }
            }

            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == LEQ)
        {
            for (int i = 0; i < table->blocksInBuckets.size(); i++)
            {
                if (table->blocksInBuckets[i].size())
                {
                    if (table->bucketRanges[i].first <= parsedQuery.selectionIntLiteral)
                        retrieveResult(table, resultantTable, i);
                    else
                        break;
                }
            }   

            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == GREATER_THAN)
        {
            for (int i = table->blocksInBuckets.size() - 1; i >= 0; i--)
            {
                if (table->blocksInBuckets[i].size())
                {
                    if (table->bucketRanges[i].second > parsedQuery.selectionIntLiteral)
                        retrieveResult(table, resultantTable, i);
                    else
                        break;
                }
            }

            saveResult(resultantTable);
            return;
        }
        else if (parsedQuery.selectionBinaryOperator == GEQ)
        {
            for (int i = table->blocksInBuckets.size() - 1; i >= 0; i--)
            {
                if (table->blocksInBuckets[i].size())
                {
                    if (table->bucketRanges[i].second >= parsedQuery.selectionIntLiteral)
                        retrieveResult(table, resultantTable, i);
                    else
                        break;
                }
            } 

            saveResult(resultantTable);
            return;
        }
    }
}