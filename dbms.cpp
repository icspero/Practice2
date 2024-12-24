#include "header.h"

#include <iostream>
#include <algorithm>
#include <sstream>
#include <string>
#include "json.hpp"
#include <fstream>
#include <filesystem>
#include <iomanip>
#include <locale>
#include "Utils.h"

using namespace std;
using json = nlohmann::json;
namespace fs = std::filesystem;

int dbmsQueries(string& command, ostringstream& responseStream) {

    setlocale(LC_ALL, "ru");

    string schmName = "schema.json";

    bool dir;
    ifstream configFile(schmName);
    json config;
    configFile >> config;
    configFile.close();
    string name = config["name"];
    if (fs::exists(name) && fs::is_directory(name)){
        dir = true;
    } else {
        dir = false;
    }

    if (dir == false) {
        dir = true;
        responseStream << endl;
        createSchemaStructure(schmName, responseStream);
        responseStream << endl;
    }
    
    if (command == "exit") {
        return false;
    }

    if (command.find("CREATE") != string::npos) {
        responseStream << endl;
        createSchemaStructure(schmName, responseStream);
        responseStream << endl;
    }
    if(command.find("SELECT") != string::npos){
        string schema = schemaName(schmName);
        string select;
        size_t wherePos = command.find("WHERE");
        if (wherePos != string::npos) {
            select = command.substr(0, wherePos - 1);
        } else {
            select = command;
        }
        lockTablesFromQuery(select, 1);
        RowNode* table = processSelectQuery(select, schema, responseStream);
        string cond;
        if (wherePos != string::npos) {
            cond = command.substr(wherePos + 6);
        } else {
            cond = " ";
        }
        RowNode* result = selectFiltration(table, cond);
        responseStream << endl << "Результат: " << endl;
        printTableSecond(result, responseStream);
        lockTablesFromQuery(select, 0);
        freeOneTable(table);
        freeOneTable(result);
    }
    if (command.find("DELETE") != string::npos){
        string schema = schemaName(schmName);
        lockTablesFromQuery(command, 1);
        string tableName = extractName(command);
        RowNode* table = convertCSVToLinkedList(schema + "/" + tableName, responseStream);
        responseStream << "WHERE ";
        string cond;
        getline(cin, cond);
        addTableNames(table, tableName);
        addColumnNames(table);
        RowNode* result = deleteFrom(table, cond);
        responseStream << endl << "Результат: " << endl;
        printTableSecond(result, responseStream);
        convertToCSV(result, schema + "/" + tableName, responseStream);
        lockTablesFromQuery(command, 0);
        freeOneTable(table);
        freeOneTable(result);
    }
    if (command.find("INSERT") != string::npos){
        int tuples = tuplesLimit(schmName);
        lockTablesFromQuery(command, 1);
        string tableName = extractName(command);
        responseStream << "VALUES ";
        string values;
        getline(cin, values);
        string listString[50];       
        parseValues(values, listString);
        RowNode* result = insertInto(nullptr, listString, tableName, tuples, schmName, responseStream);
        lockTablesFromQuery(command, 0);
        freeOneTable(result);
    }
     
	return 0;
}