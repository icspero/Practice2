#pragma once

#include "Node.h"
#include "sqlExpressions.h"
#include <string>
using namespace std;

void freeOneTable(RowNode* table);
void freeAllTables(RowNode** tables, int count);
void printTableSecond(RowNode* table, ostringstream& responseStream);
int tuplesLimit(const string& configPath);
string schemaName(const string& configPath);
void createSchemaStructure(const string& configPath, ostringstream& responseStream);
int readPrimaryKey(const string& tablePath, const string& tableName);
void updatePrimaryKey(const string& tablePath, const string& tableName, int newPrimaryKey);
void appendRowToCSV(const string& filePath, const string listString[], int numCols);
void appendRow(RowNode*& tail, Node* rowHead);
RowNode* convertCSVToLinkedList(const string& directoryPath, ostringstream& responseStream);
void convertToCSV(RowNode* table, const string& directory, ostringstream& responseStream);
void parseQuery(const string& query, string*& tables, int& tableCount, string**& columns, int*& columnCounts);
void addColumnNames(RowNode* rows);
void addTableNames(RowNode* rows, const string& tableName);
RowNode* processSelectQuery(const string& query, const string& schemaPath, ostringstream& responseStream);
void lockTables(const string& tableName, bool parameter);
void extractTableNames(const string& query, string*& tables, int& tableCount);
string extractName(const string& query);
void lockTablesFromQuery(const string& query, int lockNum);
void parseValues(const string& input, string listString[]);
