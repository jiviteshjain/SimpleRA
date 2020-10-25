#include"global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT(){
    logger.log("syntacticParseSORT");
    if((tokenizedQuery.size() != 8 && tokenizedQuery.size() != 10) || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN" || (tokenizedQuery.size() == 10 && tokenizedQuery[8] != "BUFFER")){
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    parsedQuery.sortColumnName = tokenizedQuery[5];
    if (tokenizedQuery.size() == 10)
        parsedQuery.sortBufferSize = stoi(tokenizedQuery[9]);

    string sortingStrategy = tokenizedQuery[7];
    if(sortingStrategy == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if(sortingStrategy == "DESC")
        parsedQuery.sortingStrategy = DESC;
    else{
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    return true;
}

bool semanticParseSORT(){
    logger.log("semanticParseSORT");

    if(tableCatalogue.isTable(parsedQuery.sortResultRelationName)){
        cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isTable(parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Relation doesn't exist"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    return true;
}

void executeSORT()
{
    Table table = *tableCatalogue.getTable(parsedQuery.sortRelationName);

    int runSize = table.maxRowsPerBlock * parsedQuery.sortBufferSize;
    int numberOfRuns = ceil((float)table.rowCount / (float)runSize);

    Cursor cursor = table.getCursor();
    vector<int> row = cursor.getNext();
    int columnIndex = table.getColumnIndex(parsedQuery.sortColumnName);
    int runCount = 0;
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
                [&](const std::vector<int> &a, const std::vector<int> &b) {
                    if (parsedQuery.sortingStrategy == ASC)
                        return a[columnIndex] < b[columnIndex];
                    else if (parsedQuery.sortingStrategy == DESC)
                        return a[columnIndex] >= b[columnIndex];
                });

        int rowsWritten = 0;
        int blocksWritten = 0;
        while (rowsWritten < rowsRead)
        {
            vector<vector<int>> subvector;
            if (rowsWritten + table.maxRowsPerBlock <= rowsRead)
            {
                subvector = {rows.begin() + rowsWritten, rows.begin() + rowsWritten + table.maxRowsPerBlock};
                rowsWritten += table.maxRowsPerBlock;
            }
            else
            {
                subvector = {rows.begin() + rowsWritten, rows.end()};
                rowsWritten = rowsRead;
            }
            
            bufferManager.writeTablePage(table.tableName + "_Run_" + to_string(runCount), blocksWritten++, subvector, subvector.size());
        }
        runCount++;
    }

    return;
}   