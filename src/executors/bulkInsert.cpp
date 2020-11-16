#include "global.h"
/**
 * @brief 
 * SYNTAX: BULK_INSERT <csv_file_name> INTO <table_name>
 */
bool syntacticParseBULKINSERT()
{
    logger.log("syntacticParseBULKINSERT");
    if (tokenizedQuery.size() < 4 || tokenizedQuery[2] != "INTO")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = BULKINSERT;
    parsedQuery.bulkInsertFileName = "../data/" + tokenizedQuery[1] + ".csv";
    parsedQuery.bulkInsertRelationName = tokenizedQuery[3];

    return true;
}

bool semanticParseBULKINSERT()
{
    logger.log("semanticParseBULKINSERT");

    if (!tableCatalogue.isTable(parsedQuery.bulkInsertRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    Table *table = tableCatalogue.getTable(parsedQuery.bulkInsertRelationName);

    vector<string> columns;
    fstream fin(parsedQuery.bulkInsertFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        string word;
        stringstream s(line);
        while (getline(s, word, ','))
        {
            word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
            columns.emplace_back(word);
        }
    }

    if (columns != table->columns)
    {
        if (columns.empty())
            cout << "SEMANTIC ERROR: File doesn't exist" << endl;
        else
            cout << "SEMANTIC ERROR: Column mismatch" << endl;
        return false;
    }
    return true;
}

void executeBULKINSERT()
{
    logger.log("executeBULKINSERT");
    Table *table = tableCatalogue.getTable(parsedQuery.bulkInsertRelationName);

    ifstream fin(parsedQuery.bulkInsertFileName, ios::in);
    string line, word;
    vector<int> row(table->columnCount, 0);

    getline(fin, line);

    if (table->indexed == false)
    {
        vector<vector<int>> rowsInPage(table->maxRowsPerBlock, row);
        int pageCounter = 0;
        
        if (table->maxRowsPerBlock - table->rowsPerBlockCount[table->blockCount - 1] != 0)
        {
            while (getline(fin, line))
            {
                stringstream s(line);
                for (int columnCounter = 0; columnCounter < table->columnCount; columnCounter++)
                {
                    if (!getline(s, word, ','))
                        return;
                    row[columnCounter] = stoi(word);
                    rowsInPage[pageCounter][columnCounter] = row[columnCounter];
                }
                pageCounter++;
                table->updateStatistics(row);
                if (pageCounter == table->maxRowsPerBlock - table->rowsPerBlockCount[table->blockCount - 1])
                {
                    vector<vector<int>> rowsInLastPage = bufferManager.getTablePage(table->tableName, table->blockCount - 1).data;
                    rowsInLastPage.resize(table->rowsPerBlockCount[table->blockCount - 1]);
                    rowsInLastPage.insert(std::end(rowsInLastPage), std::begin(rowsInPage), std::end(rowsInPage));
                    bufferManager.writeTablePage(table->tableName, table->blockCount - 1, rowsInLastPage, table->maxRowsPerBlock);
                    table->rowsPerBlockCount[table->blockCount - 1] = table->maxRowsPerBlock;
                    pageCounter = 0;
                    break;
                }
            }
        }

        while (getline(fin, line))
        {
            stringstream s(line);
            for (int columnCounter = 0; columnCounter < table->columnCount; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return;
                row[columnCounter] = stoi(word);
                rowsInPage[pageCounter][columnCounter] = row[columnCounter];
            }
            pageCounter++;
            table->updateStatistics(row);
            if (pageCounter == table->maxRowsPerBlock)
            {
                bufferManager.writeTablePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
                table->blockCount++;
                table->rowsPerBlockCount.emplace_back(pageCounter);
                pageCounter = 0;
            }
        }

        if (pageCounter <= table->maxRowsPerBlock - table->rowsPerBlockCount[table->blockCount - 1])
        {
            vector<vector<int>> rowsInLastPage = bufferManager.getTablePage(table->tableName, table->blockCount - 1).data;
            rowsInLastPage.resize(table->rowsPerBlockCount[table->blockCount - 1]);
            rowsInLastPage.insert(std::end(rowsInLastPage), std::begin(rowsInPage), std::end(rowsInPage));
            bufferManager.writeTablePage(table->tableName, table->blockCount - 1, rowsInLastPage, table->rowsPerBlockCount[table->blockCount - 1] + pageCounter);
            table->rowsPerBlockCount[table->blockCount - 1] = table->rowsPerBlockCount[table->blockCount - 1] + pageCounter;
            pageCounter = 0;
        }
        else if (pageCounter < table->maxRowsPerBlock)
        {
            bufferManager.writeTablePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
            table->blockCount++;
            table->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }
    }
    else if (table->indexingStrategy == HASH || table->indexingStrategy == BTREE)
    {
        while (getline(fin, line))
        {
            stringstream s(line);
            for (int columnCounter = 0; columnCounter < table->columnCount; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return;
                row[columnCounter] = stoi(word);
            }

            table->insert(row);
        }
    }
}