#include "global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */

// #define sim template < class c
// #define ris return * this
// #define dor > debug & operator <<
// #define eni(x) sim > typename \
//   enable_if<sizeof dud<c>(0) x 1, debug&>::type operator<<(c i) {
// sim > struct rge { c b, e; };
// sim > rge<c> range(c i, c j) { return rge<c>{i, j}; }
// sim > auto dud(c* x) -> decltype(cerr << *x, 0);
// sim > char dud(...);
// struct debug {
// #ifdef LOCAL
// ~debug() { cerr << endl; }
// eni(!=) cerr << boolalpha << i; ris; }
// eni(==) ris << range(begin(i), end(i)); }
// sim, class b dor(pair < b, c > d) {
//   ris << "(" << d.first << ", " << d.second << ")";
// }
// sim dor(rge<c> d) {
//   *this << "[";
//   for (auto it = d.b; it != d.e; ++it)
//   *this << ", " + 2 * (it == d.b) << *it;
//   ris << "]";
// }
// #else
// sim dor(const c&) { ris; }
// #endif
// };
// #define imie(...) " [" << #VA_ARGS ": " << (VA_ARGS) << "] "

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

    int runSize = table->maxRowsPerBlock * parsedQuery.sortBufferSize;

    Cursor cursor = table->getCursor();
    vector<int> row = cursor.getNext();
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
            row = cursor.getNext();
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
            if (rowsWritten + table->maxRowsPerBlock <= rowsRead)
            {
                subvector = {rows.begin() + rowsWritten, rows.begin() + rowsWritten + table->maxRowsPerBlock};
                rowsWritten += table->maxRowsPerBlock;
            }
            else
            {
                subvector = {rows.begin() + rowsWritten, rows.end()};
                rowsWritten = rowsRead;
            }

            bufferManager.writeTablePage(table->tableName, table->blockCount + blocksWritten++, subvector, subvector.size());
            table->rowsPerBlockCount.emplace_back(subvector.size());
            pagesInRun[runsCount]++;
        }
        runsCount++;
        zerothPassRunsCount = runsCount;
    }

    table->blockCount += table->blockCount;

    int passCount = 0;
    int totalPasses = ceil(log(runsCount) / log(parsedQuery.sortBufferSize - 1));

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
                int runPageIndex;

                if (passCount & 1)
                    runPageIndex = 2 * originalBlockCount;
                else
                    runPageIndex = originalBlockCount;

                runPageIndex += ((runsRead + i) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + i)]++);
                dataFromPages[i] = (bufferManager.getTablePage(table->tableName, runPageIndex).data);
                heap.push(make_pair(dataFromPages[i][rowsReadFromPages[i]++], i));
            }
            vector<vector<int>> rows;
            while (!heap.empty())
            {
                pair<vector<int>, int> temp = heap.top();
                heap.pop();

                rows.push_back(temp.first);
                if (rows.size() == table->maxRowsPerBlock)
                {
                    int runPageIndex;

                    if (passCount & 1)
                        runPageIndex = originalBlockCount;
                    else
                        runPageIndex = 2 * originalBlockCount;
                    runPageIndex += blocksWritten++;
                    if (passCount == totalPasses - 1)
                    {
                        for (auto row : rows)
                            resultantTable->writeRow<int>(row);
                    }
                    else
                    {
                        bufferManager.writeTablePage(table->tableName, runPageIndex, rows, rows.size());
                        table->rowsPerBlockCount.emplace_back(rows.size());
                        table->blockCount++;
                    }

                    pagesInRun[(passCount + 1) * zerothPassRunsCount + runsWritten]++;
                    rows.clear();
                }

                int runPageIndex;

                if (passCount & 1)
                    runPageIndex = 2 * originalBlockCount;
                else
                    runPageIndex = originalBlockCount;

                runPageIndex += ((runsRead + temp.second) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)] - 1);
                if (rowsReadFromPages[temp.second] != table->rowsPerBlockCount[runPageIndex])
                    heap.push(make_pair(dataFromPages[temp.second][rowsReadFromPages[temp.second]++], temp.second));
                else //all rows exhausted
                {
                    if (pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)] != pagesInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)]) // all pages not exhausted
                    {
                        int runPageIndex = (passCount & 1 ? 2 * originalBlockCount : originalBlockCount);
                        runPageIndex += ((runsRead + temp.second) * pagesInRun[passCount * zerothPassRunsCount] + pagesReadInRun[passCount * zerothPassRunsCount + (runsRead + temp.second)]++);
                        dataFromPages[temp.second] = bufferManager.getTablePage(table->tableName, runPageIndex).data;
                        rowsReadFromPages[temp.second] = 0;
                        heap.push(make_pair(dataFromPages[temp.second][rowsReadFromPages[temp.second]++], temp.second));
                    }
                }
            }
            if (rows.size())
            {
                int runPageIndex;

                if (passCount & 1)
                    runPageIndex = originalBlockCount;
                else
                    runPageIndex = 2 * originalBlockCount;
                runPageIndex += blocksWritten++;

                if (passCount == totalPasses - 1)
                {
                    for (auto row : rows)
                        resultantTable->writeRow<int>(row);
                }
                else
                {
                    bufferManager.writeTablePage(table->tableName, runPageIndex, rows, rows.size());
                    table->rowsPerBlockCount.emplace_back(rows.size());
                    table->blockCount++;
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

    vector<vector<int>> rows;

    for (int i = originalBlockCount; i < table->blockCount; i++)
    {
        if (totalPasses == 0) //buffer was large enough, only one run was created, while loop was never called
            for (auto row : bufferManager.getTablePage(table->tableName, i).data)
                resultantTable->writeRow<int>(row);
        bufferManager.deleteTableFile(table->tableName, i);
    }

    table->blockCount = originalBlockCount;

    if (resultantTable->blockify())
        tableCatalogue.insertTable(resultantTable);
    else
    {
        cout << "Empty Table" << endl;
        resultantTable->unload();
        delete resultantTable;
    }

    return;
}