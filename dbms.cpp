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
        string deleteF;
        size_t wherePos = command.find("WHERE");
        if (wherePos != string::npos) {
            deleteF = command.substr(0, wherePos - 1);
        } else {
            deleteF = command;
        }
        lockTablesFromQuery(deleteF, 1);
        string tableName = extractName(deleteF);
        RowNode* table = convertCSVToLinkedList(schema + "/" + tableName, responseStream);
        string cond;
        if (wherePos != string::npos) {
            cond = command.substr(wherePos + 6);
        } else {
            cond = " ";
        }
        addTableNames(table, tableName);
        addColumnNames(table);
        RowNode* result = deleteFrom(table, cond);
        responseStream << endl << "Результат: " << endl;
        printTableSecond(result, responseStream);
        convertToCSV(result, schema + "/" + tableName, responseStream);
        lockTablesFromQuery(deleteF, 0);
        freeOneTable(table);
        freeOneTable(result);
    }
    if (command.find("INSERT") != string::npos){
        int tuples = tuplesLimit(schmName);
        string insert;
        size_t value = command.find("VALUES");
        if (value != string::npos) {
            insert = command.substr(0, value - 1);
        } else {
            insert = command;
        }
        lockTablesFromQuery(insert, 1);
        string tableName = extractName(insert);
        string values;
        if (value != string::npos) {
            values = command.substr(value + 7);
        } else {
            values = " ";
        }
        string listString[50];       
        parseValues(values, listString);
        RowNode* result = insertInto(nullptr, listString, tableName, tuples, schmName, responseStream);
        lockTablesFromQuery(insert, 0);
        freeOneTable(result);
    }
     
	return 0;
}