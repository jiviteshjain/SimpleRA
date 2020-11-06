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
}