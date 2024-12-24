#pragma once

#include "Node.h"
#include "Utils.h"
#include <string>
using namespace std;

RowNode* insertInto(RowNode* table, string listString[], const string& tableName, int tuplesLimit, const string& configPath, ostringstream& responseStream);
RowNode* cartesianProduct(RowNode** tables, int countTables);
RowNode* selectFromTable(RowNode* table, string columns[], int& columnCount, const string& tableName);
Node* split(const string& input, const string& delimiter);
bool checkConditions(RowNode* baseDataLine, const string& filter);
RowNode* selectFiltration(RowNode* table, const string& filter);
RowNode* deleteFrom(RowNode* table, const string& filter);
