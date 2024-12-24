#include "sqlExpressions.h"
#include "Utils.h"
#include "header.h"

#include <iostream>
#include <algorithm>
#include <sstream>
#include <string>
#include "json.hpp"
#include <fstream>
#include <filesystem>
#include <iomanip>

using namespace std;
using json = nlohmann::json;
namespace fs = std::filesystem;

void freeOneTable(RowNode* table) {
    while (table != nullptr) {
        RowNode* tempRow = table;
        table = table->nextRow;

        Node* currNode = tempRow->cell;
        while (currNode != nullptr) {
            Node* tempNode = currNode;
            currNode = currNode->next;
            delete tempNode;
        }
        delete tempRow;
    }
}

void freeAllTables(RowNode** tables, int count) {
    for (int i = 0; i < count; ++i) {
        freeOneTable(tables[i]); 
    }
}

void printTableSecond(RowNode* table, ostringstream& responseStream) {
    if (table == nullptr) return;

    Node* header = table->cell;
    int maxColumnWidths[50] = {0}; 
    int colCount = 0;

    while (header != nullptr) {
        maxColumnWidths[colCount] = header->name.length(); 
        header = header->next;
        colCount++;
    }

    RowNode* row = table->nextRow; 
    while (row != nullptr) {
        Node* cell = row->cell;
        int colIdx = 0;
        while (cell != nullptr && colIdx < colCount) {
            int width = cell->cell.length();
            if (width > maxColumnWidths[colIdx]) {
                maxColumnWidths[colIdx] = width;
            }
            cell = cell->next;
            colIdx++;
        }
        row = row->nextRow;
    }

    auto printSeparator = [&](bool isEnd) {
        for (int i = 0; i < colCount; i++) {
            responseStream << "+";
            for (int j = 0; j < maxColumnWidths[i] + 2; j++) {
                responseStream << "-";
            }
        }
        responseStream << "+\n";
        if (isEnd) return;
    };

    printSeparator(false);
    header = table->cell;
    responseStream << "|";
    for (int i = 0; i < colCount; i++) {
        responseStream << " " << setw(maxColumnWidths[i]) << left << header->name << " |";
        header = header->next;
    }
    responseStream << "\n";
    printSeparator(false);

    row = table->nextRow; 
    while (row != nullptr) {
        Node* cell = row->cell;
        responseStream << "|";
        for (int i = 0; i < colCount; i++) {
            if (cell != nullptr) {
                responseStream << " " << setw(maxColumnWidths[i]) << left << cell->cell << " |";
                cell = cell->next;
            } else {
                responseStream << " " << setw(maxColumnWidths[i]) << " " << " |"; 
            }
        }
        responseStream << "\n";
        if (row->nextRow != nullptr) {
            printSeparator(false); 
        }
        row = row->nextRow;
    }
    printSeparator(true);
}

int tuplesLimit(const string& configPath){

	ifstream configFile(configPath);

    json config;
    configFile >> config;
    configFile.close();

	int tuples_Limit = config["tuples_limit"];
	
	return tuples_Limit;
}

string schemaName(const string& configPath){

	ifstream configFile(configPath);

    json config;
    configFile >> config;
    configFile.close();

	string name = config["name"];
	
	return name;
}

void createSchemaStructure(const string& configPath, ostringstream& responseStream) {
    
    ifstream configFile(configPath);
    if (!configFile.is_open()) {
        cerr << "Не удалось открыть файл конфигурации: " << configPath << endl;
        return;
    }

    json config;
    configFile >> config;
    configFile.close();

    string schemaName = config["name"];
    int tuplesLimit = config["tuples_limit"];
    auto structure = config["structure"];

    fs::create_directory(schemaName);

    for (auto& [tableName, columns] : structure.items()) {
        string tableDir = schemaName + "/" + tableName;

        fs::create_directory(tableDir);

        ofstream csvFile(tableDir + "/1.csv");
        if (!csvFile.is_open()) {
            cerr << "Не удалось создать файл для таблицы: " << tableName << endl;
            continue;
        }

        for (size_t i = 0; i < columns.size(); ++i) {
            csvFile << columns[i];
            if (i < columns.size() - 1) {
                csvFile << ",";
            }
        }
        csvFile << "\n";
        csvFile.close();
    }

    responseStream << "Директории и файлы успешно созданы для схемы: " << schemaName << endl;
}

int readPrimaryKey(const string& tablePath, const string& tableName) {
    ifstream pkFile(tablePath + "/" + tableName + "_pk_sequence");
    int primaryKey = 0;
    if (pkFile.is_open()) {
        pkFile >> primaryKey;
        pkFile.close();
    }
    return primaryKey;
}

void updatePrimaryKey(const string& tablePath, const string& tableName, int newPrimaryKey) {
    ofstream pkFile(tablePath + "/" + tableName + "_pk_sequence", ios::trunc);
    if (pkFile.is_open()) {
        pkFile << newPrimaryKey;
        pkFile.close();
    }
}

void appendRowToCSV(const string& filePath, const string listString[], int numCols) {
    ofstream csvFile(filePath, ios::app);
    if (csvFile.is_open()) {
        for (int i = 0; i < numCols; i++) {
            csvFile << listString[i];
            if (i < numCols - 1) csvFile << ",";
        }
        csvFile << "\n";
        csvFile.close();
    }
}

void appendRow(RowNode*& tail, Node* rowHead) {
    RowNode* newRow = new RowNode;
    newRow->cell = rowHead;
    newRow->nextRow = nullptr;

    if (tail) {
        tail->nextRow = newRow;
    }
    tail = newRow;
}

RowNode* convertCSVToLinkedList(const string& directoryPath, ostringstream& responseStream) {
    RowNode* head = nullptr;
    RowNode* tail = nullptr;
    bool isHeaderLoaded = false;
    string line;

    for (int i = 1; ; ++i) {
        string filePath = directoryPath + "/" + to_string(i) + ".csv";
        if (!fs::exists(filePath)) {
            break;
        }

        ifstream file(filePath);
        if (!file.is_open()) {
            cerr << "Не удалось открыть файл!" << filePath << endl;
            continue;
        }

        bool isFirstLine = true;
        while (getline(file, line)) {
            if (isFirstLine && isHeaderLoaded) {
                isFirstLine = false;
                continue;
            }

            istringstream ss(line);
            string cell;
            Node* rowHead = nullptr;
            Node* rowTail = nullptr;
            int colIndex = 0;

            while (getline(ss, cell, ',')) {
                if (!cell.empty() && cell.front() == '"' && cell.back() == '"') {
                    cell = cell.substr(1, cell.size() - 2);
                }

                Node* newNode = new Node;
                newNode->numColumn = colIndex++;
                newNode->cell = cell;
                newNode->next = nullptr;

                if (isFirstLine && !isHeaderLoaded) {
                    newNode->name = cell; 
                } else {
                    newNode->name = "колонка" + to_string(newNode->numColumn + 1);
                }

                if (rowHead == nullptr) {
                    rowHead = newNode;
                } else {
                    rowTail->next = newNode;
                }
                rowTail = newNode;
            }

            RowNode* newRow = new RowNode;
            newRow->cell = rowHead;
            newRow->nextRow = nullptr;

            if (isFirstLine && !isHeaderLoaded) {
                head = newRow;
                tail = head;
                isHeaderLoaded = true;
            } else {
                appendRow(tail, rowHead);
                if (head == nullptr) {
                    head = tail;
                }
            }

            isFirstLine = false;
        }

        file.close();
    }

    return head;
}

void convertToCSV(RowNode* table, const string& directory, ostringstream& responseStream) {
    int tuples = tuplesLimit("schema.json");
    int rowCount = 0;
    int fileIndex = 1;
    RowNode* currentRow = table->nextRow; 

    Node* headerCell = table->cell;
    string headerLine;
    while (headerCell != nullptr) {
        headerLine += "" + headerCell->name + "";
        headerCell = headerCell->next;
        if (headerCell != nullptr) {
            headerLine += ",";
        }
    }

    while (currentRow != nullptr) {
        string filename = directory + "/" + to_string(fileIndex) + ".csv";
        ofstream csvFile(filename, ios::out | ios::trunc);

        if (!csvFile.is_open()) {
            cerr << "Не удалось открыть файл: " << filename << endl;
            return;
        }

        csvFile << headerLine << "\n";

        for (int i = 0; i < tuples && currentRow != nullptr; i++) {
            Node* currentCell = currentRow->cell;
            while (currentCell != nullptr) {
                csvFile << currentCell->cell;
                currentCell = currentCell->next;
                if (currentCell != nullptr) {
                    csvFile << ",";
                }
            }
            csvFile << "\n";
            currentRow = currentRow->nextRow;
            rowCount++;
        }

        csvFile.close();
        fileIndex++;
    }

    while (fs::exists(directory + "/" + to_string(fileIndex) + ".csv")) {
        fs::remove(directory + "/" + to_string(fileIndex) + ".csv");
        fileIndex++;
    }
}

void parseQuery(const string& query, string*& tables, int& tableCount, string**& columns, int*& columnCounts) {
    size_t selectPos = query.find("SELECT ");
    size_t fromPos = query.find(" FROM ");

    string columnsStr = query.substr(selectPos + 7, fromPos - (selectPos + 7));
    string tablesStr = query.substr(fromPos + 6);

    tableCount = 0;
    size_t currentPos = 0;

    while (currentPos < tablesStr.size()) {
        size_t commaPos = tablesStr.find(",", currentPos);
        if (commaPos == string::npos) commaPos = tablesStr.size();
        tableCount++;
        currentPos = commaPos + 2; 
    }

    tables = new string[tableCount];
    columns = new string*[tableCount];
    columnCounts = new int[tableCount];

    currentPos = 0;
    int tableIndex = 0;
    while (currentPos < tablesStr.size()) {
        size_t commaPos = tablesStr.find(",", currentPos);
        if (commaPos == string::npos) commaPos = tablesStr.size();

        tables[tableIndex++] = tablesStr.substr(currentPos, commaPos - currentPos);
        currentPos = commaPos + 2; 
    }

    for (int i = 0; i < tableCount; i++) {
        columns[i] = new string[100];
        columnCounts[i] = 0;
    }

    currentPos = 0;
    while (currentPos < columnsStr.size()) {
        size_t commaPos = columnsStr.find(",", currentPos);
        if (commaPos == string::npos) commaPos = columnsStr.size();

        string column = columnsStr.substr(currentPos, commaPos - currentPos);

        size_t dotPos = column.find(".");
        string tableName = column.substr(0, dotPos);
        string columnName = column.substr(dotPos + 1);

        for (int i = 0; i < tableCount; i++) {
            if (tables[i] == tableName) {
                columns[i][columnCounts[i]++] = columnName;
                break;
            }
        }

        currentPos = commaPos + 2;
    }
}

void addColumnNames(RowNode* rows) {
    RowNode* headerRow = rows; 

    if (headerRow == nullptr) {
        return; 
    }

    Node* headerCell = headerRow->cell;

    for (RowNode* row = headerRow->nextRow; row != nullptr; row = row->nextRow) {
        Node* currentNode = row->cell;
        Node* headerNode = headerCell;

        while (currentNode != nullptr && headerNode != nullptr) {
            currentNode->name = headerNode->cell; 
            currentNode = currentNode->next;
            headerNode = headerNode->next;
        }
    }
}

void addTableNames(RowNode* rows, const string& tableName) {
    for (RowNode* row = rows; row != nullptr; row = row->nextRow) {
        for (Node* currentNode = row->cell; currentNode != nullptr; currentNode = currentNode->next) {
            currentNode->tableName = tableName;
        }
    }
}

RowNode* processSelectQuery(const string& query, const string& schemaPath, ostringstream& responseStream) {
    string* tables = nullptr;
    int tableCount = 0;
    string** columns = nullptr;
    int* columnCounts = nullptr;

    parseQuery(query, tables, tableCount, columns, columnCounts);

    RowNode** tableArray = new RowNode*[tableCount];

    for (int i = 0; i < tableCount; i++) {
        RowNode* table = convertCSVToLinkedList(schemaPath + "/" + tables[i], responseStream);

        string* columnNames = nullptr;
        if (columnCounts[i] == 0) {
            columnNames = new string[1];
            columnNames[0] = "";
        } else {
            columnNames = columns[i];
        }

        tableArray[i] = selectFromTable(table, columnNames, columnCounts[i], tables[i]);

        if (columnCounts[i] == 0) {
            delete[] columnNames;
        }
    }

    RowNode* result = nullptr;
    if (tableCount > 1) {
        result = cartesianProduct(tableArray, tableCount);
    } else {
        result = tableArray[0];
    }

    for (int i = 0; i < tableCount; i++) {
        delete[] columns[i];
    }
    delete[] columns;
    delete[] tables;
    delete[] columnCounts;
    delete[] tableArray;

    return result;
}

void lockTables(const string& tableName, bool parameter) {

    string schema = schemaName("schema.json");
    string directory = schema + "/" + tableName;
    string fileName = directory + "/" + tableName + "_lock.txt";
    
    filesystem::create_directories(directory);
    
    ofstream lockFile(fileName, ios::binary);
    if (lockFile.is_open()) {
        lockFile << 0;
        lockFile.close();
    } else {
        cerr << "Ошибка создания файла блокировки!" << endl;
        return;
    }

    ifstream inputFile(fileName);
    if (!inputFile) {
        cerr << "Ошибка открытия файла для чтения!" << endl;
        return;
    }

    string currValueStr;
    inputFile >> currValueStr;
    inputFile.close();

    int currValue = stoi(currValueStr);
    if (parameter) {
        currValue++;
    } else {
        currValue--;
    }

    ofstream outputFile(fileName);
    if (!outputFile) {
        cerr << "Ошибка открытия файла для записи!" << endl;
        return;
    }

    outputFile << currValue;
    outputFile.close();
}

void extractTableNames(const string& query, string*& tables, int& tableCount) {
    const string keywords[] = {"FROM", "INTO"};
    size_t keywordPos = string::npos;

    for (const string& keyword : keywords) {
        keywordPos = query.find(keyword);
        if (keywordPos != string::npos) {
            keywordPos += keyword.length();
            break;
        }
    }

    if (keywordPos == string::npos) {
        tableCount = 0;
        tables = nullptr;
        return;
    }

    string tablesPart = query.substr(keywordPos);
    
    while (!tablesPart.empty() && isspace(tablesPart.front())) {
        tablesPart.erase(tablesPart.begin());
    }
    while (!tablesPart.empty() && isspace(tablesPart.back())) {
        tablesPart.pop_back();
    }

    tableCount = 1; 
    for (size_t i = 0; i < tablesPart.length(); i++) {
        if (tablesPart[i] == ',' && tablesPart[i + 1] == ' ') {
            tableCount++;
        }
    }

    tables = new string[tableCount];

    size_t start = 0, end = 0, currentIndex = 0;
    while ((end = tablesPart.find(", ", start)) != string::npos) {
        tables[currentIndex++] = tablesPart.substr(start, end - start);
        start = end + 2;
    }
    tables[currentIndex] = tablesPart.substr(start);
}

string extractName(const string& query) {
    string keywords[] = {"INTO", "FROM"};
    size_t position = string::npos;

    for (const string& keyword : keywords) {
        position = query.find(keyword);
        if (position != string::npos) {
            position += keyword.length() + 1; 
            break;
        }
    }

    if (position == string::npos) {
        return ""; 
    }

    size_t endPos = query.find(' ', position);
    if (endPos == string::npos) {
        endPos = query.length();
    }

    return query.substr(position, endPos - position);
}

void lockTablesFromQuery(const string& query, int lockNum) {
    string* tables = nullptr;
    int tableCount = 0;

    extractTableNames(query, tables, tableCount);

    for (int i = 0; i < tableCount; i++) {
        lockTables(tables[i], lockNum);
    }

    delete[] tables;
}

void parseValues(const string& input, string listString[]) {
    
    int count = 0;
    listString[0] = "";
    size_t startPos = input.find('(');
    if (startPos == string::npos) {
        count = 0;
        return;
    }

    size_t endPos = input.find(')', startPos);
    if (endPos == string::npos) {
        count = 0;
        return;
    }

    string values = input.substr(startPos + 1, endPos - startPos - 1);

    count = 1;
    size_t pos = 0;
    while ((pos = values.find(',')) != string::npos) {
        string value = values.substr(0, pos);
        value.erase(0, value.find_first_not_of(" '"));
        value.erase(value.find_last_not_of(" '") + 1);

        listString[count++] = value;
        values.erase(0, pos + 1);
    }

    values.erase(0, values.find_first_not_of(" '"));
    values.erase(values.find_last_not_of(" '") + 1);
    listString[count++] = values;
}