#include "global.h"

bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");
    if ((tokenizedQuery.size() != 8 && tokenizedQuery.size() != 10) || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN" || (tokenizedQuery.size() == 10 && tokenizedQuery[8] != "BUFFER"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    parsedQuery.sortColumnName = tokenizedQuery[5];
    if (tokenizedQuery.size() == 10)
        parsedQuery.sortBufferSize = stoi(tokenizedQuery[9]);

    string sortingStrategy = tokenizedQuery[7];
    if (sortingStrategy == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if (sortingStrategy == "DESC")
        parsedQuery.sortingStrategy = DESC;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (tableCatalogue.isTable(parsedQuery.sortResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    return true;
}

void executeSORT()
{
    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    Table *resultantTable = new Table(parsedQuery.sortResultRelationName, table->columns);
    resultantTable->blockCount = table->blockCount;
    resultantTable->columnCount = table->columnCount;
    resultantTable->columns = table->columns;
    // resultantTable->valuesInColumns = table->valuesInColumns;
    resultantTable->smallestInColumns = table->smallestInColumns;
    resultantTable->largestInColumns = table->largestInColumns;
    resultantTable->maxRowsPerBlock = table->maxRowsPerBlock;
    resultantTable->rowCount = table->rowCount;
    resultantTable->rowsPerBlockCount.resize(table->blockCount * 3, 0);
    tableCatalogue.insertTable(resultantTable);

    int runSize = table->maxRowsPerBlock * parsedQuery.sortBufferSize;

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

    int columnIndex = table->getColumnIndex(parsedQuery.sortColumnName);
    int runsCount = 0;
    int zerothPassRunsCount;
    unordered_map<int, int> pagesInRun;
    int originalBlockCount = table->blockCount;
    int blocksWritten = 0;

    while (!row.empty())
    {
        vector<vector<int>> rows;
        int rowsRead = 0;
        while (!row.empty() && rowsRead != runSize)
        {
            rows.push_back(row);
            if (!table->indexed)
                row = cursor.getNext();
            else if (table->indexingStrategy == HASH)
                row = cursor.getNextInAllBuckets();

            rowsRead++;
        }

        if (rows.size())
            sort(rows.begin(), rows.end(),
                 [&](const vector<int> &a, const vector<int> &b) {
                     if (parsedQuery.sortingStrategy == ASC)
                         return a[columnIndex] < b[columnIndex];
                     else if (parsedQuery.sortingStrategy == DESC)
                         return a[columnIndex] > b[columnIndex];
                 });

        int rowsWritten = 0;
        while (rowsWritten < rowsRead)
        {
            vector<vector<int>> subvector;
            if (rowsWritten + resultantTable->maxRowsPerBlock <= rowsRead)
            {
                subvector = {rows.begin() + rowsWritten, rows.begin() + rowsWritten + resultantTable->maxRowsPerBlock};
                rowsWritten += resultantTable->maxRowsPerBlock;
            }
            else
            {
                subvector = {rows.begin() + rowsWritten, rows.end()};
                rowsWritten = rowsRead;
            }

            resultantTable->rowsPerBlockCount[table->blockCount + blocksWritten] = subvector.size();
            bufferManager.writeTablePage(resultantTable->tableName, table->blockCount + blocksWritten++, subvector, subvector.size());

            pagesInRun[runsCount]++;
            resultantTable->blockCount++;
        }
        runsCount++;
        zerothPassRunsCount = runsCount;
    }

    int passCount = 0;
    int totalPasses = ceil(log(runsCount) / log(parsedQuery.sortBufferSize - 1));
    int finalBlocksWritten = 0;
    while (passCount < totalPasses)
    {
        int runsRead = 0;
        int runsWritten = 0;
        int blocksWritten = 0;
        while (runsRead < runsCount)
        {
            unordered_map<int, int> pagesReadInRun;
            int minSize = min(runsCount - runsRead, parsedQuery.sortBufferSize - 1);

            vector<vector<vector<int>>> dataFromPages(minSize);
            vector<int> rowsReadFromPages(minSize, 0);

            auto comp = [&](const pair<vector<int>, int> &a, const pair<vector<int>, int> &b) {
                if (parsedQuery.sortingStrategy == ASC)
                    return a.first[columnIndex] > b.first[columnIndex];
                else if (parsedQuery.sortingStrategy == DESC)
                    return a.first[columnIndex] < b.first[columnIndex];
            };

            priority_queue<pair<vector<int>, int>, vector<pair<vector<int>, int>>, function<bool(const pair<vector<int>, int> &a, const pair<vector<int>, int> &b)>> heap(comp);

            for (int i = 0; i < minSize; i++)
            {
                int runPageIndex = (passCount & 1 ? 2 * originalBlockCount : originalBlockCount);

                runPageIndex += ((runsRead + i) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + i)]++);
                dataFromPages[i] = (bufferManager.getTablePage(resultantTable->tableName, runPageIndex).data);
                // dataFromPages[i].resize(resultantTable->rowsPerBlockCount[runPageIndex]);
                heap.push(make_pair(dataFromPages[i][rowsReadFromPages[i]++], i));
            }
            vector<vector<int>> rows;
            while (!heap.empty())
            {
                pair<vector<int>, int> temp = heap.top();
                heap.pop();

                rows.push_back(temp.first);
                if (rows.size() == resultantTable->maxRowsPerBlock)
                {
                    int runPageIndex = (passCount & 1 ? originalBlockCount : 2 * originalBlockCount);

                    runPageIndex += blocksWritten++;
                    if (passCount == totalPasses - 1)
                    {
                        resultantTable->rowsPerBlockCount[finalBlocksWritten] = rows.size();
                        bufferManager.writeTablePage(resultantTable->tableName, finalBlocksWritten++, rows, rows.size());
                    }
                    else
                    {
                        resultantTable->rowsPerBlockCount[runPageIndex] = rows.size();
                        bufferManager.writeTablePage(resultantTable->tableName, runPageIndex, rows, rows.size());
                        resultantTable->blockCount++;
                    }

                    pagesInRun[(passCount + 1) * zerothPassRunsCount + runsWritten]++;
                    rows.clear();
                }

                int runPageIndex = (passCount & 1 ? 2 * originalBlockCount : originalBlockCount);

                runPageIndex += ((runsRead + temp.second) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)] - 1);
                if (rowsReadFromPages[temp.second] != resultantTable->rowsPerBlockCount[runPageIndex])
                    heap.push(make_pair(dataFromPages[temp.second][rowsReadFromPages[temp.second]++], temp.second));
                else //all rows exhausted
                {
                    if (pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)] != pagesInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)]) // all pages not exhausted
                    {
                        int runPageIndex = (passCount & 1 ? 2 * originalBlockCount : originalBlockCount);
                        runPageIndex += ((runsRead + temp.second) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)]++);
                        dataFromPages[temp.second] = bufferManager.getTablePage(resultantTable->tableName, runPageIndex).data;
                        // dataFromPages[temp.second].resize(resultantTable->rowsPerBlockCount[runPageIndex]);
                        rowsReadFromPages[temp.second] = 0;
                        heap.push(make_pair(dataFromPages[temp.second][rowsReadFromPages[temp.second]++], temp.second));
                    }
                }
            }
            if (rows.size())
            {
                int runPageIndex = (passCount & 1 ? originalBlockCount : 2 * originalBlockCount);

                runPageIndex += blocksWritten++;

                if (passCount == totalPasses - 1)
                {
                    resultantTable->rowsPerBlockCount[finalBlocksWritten] = rows.size();
                    bufferManager.writeTablePage(resultantTable->tableName, finalBlocksWritten++, rows, rows.size());
                }
                else
                {
                    resultantTable->rowsPerBlockCount[runPageIndex] = rows.size();
                    bufferManager.writeTablePage(resultantTable->tableName, runPageIndex, rows, rows.size());
                    resultantTable->blockCount++;
                }

                pagesInRun[(passCount + 1) * zerothPassRunsCount + runsWritten]++;
                rows.clear();
            }
            runsRead += minSize;
            runsWritten++;
        }

        passCount++;
        runsCount = ceil((float)runsCount / (parsedQuery.sortBufferSize - 1));
    }

    for (int i = originalBlockCount; i < resultantTable->blockCount; i++)
    {
        // if (totalPasses == 0)
        //     bufferManager.writeTablePage(resultantTable->tableName, finalBlocksWritten++, bufferManager.getTablePage(resultantTable->tableName, i).data, bufferManager.getTablePage(resultantTable->tableName, i).data.size());
        if (totalPasses == 0)
            bufferManager.writeTablePage(resultantTable->tableName, finalBlocksWritten++, bufferManager.getTablePage(resultantTable->tableName, i).data, resultantTable->rowsPerBlockCount[i]);
        bufferManager.deleteTableFile(resultantTable->tableName, i);
    }

    resultantTable->blockCount = finalBlocksWritten;
    resultantTable->rowsPerBlockCount.erase(resultantTable->rowsPerBlockCount.begin() + resultantTable->blockCount, resultantTable->rowsPerBlockCount.end());
    return;
}