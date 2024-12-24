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

RowNode* insertInto(RowNode* table, string listString[], const string& tableName, int tuplesLimit, const string& configPath, ostringstream& responseStream) {
    string schema = schemaName("schema.json");
    string tablePath;
    tablePath.append(schema).append("/").append(tableName);
    
	ifstream configFile(configPath);

	json config;
	configFile >> config;
	configFile.close();

	auto structure = config["structure"];

    if (!filesystem::exists(tablePath)) {
        cerr << "Директория " << tablePath << " не существует!\n";
        return nullptr; 
    }
    
    int primaryKey = readPrimaryKey(tablePath, tableName) + 1;
    if (primaryKey == 0) {
        cerr << "Не удалось прочитать или обновить значение первичного ключа для таблицы " << tableName << "!\n";
        return nullptr;
    }
    updatePrimaryKey(tablePath, tableName, primaryKey);

    listString[0] = to_string(primaryKey);

    RowNode* newRow = new RowNode;
    newRow->cell = nullptr;
    newRow->nextRow = nullptr;

    Node* currNode = nullptr;
    for (int i = 0; listString[i] != ""; i++) {
        Node* newNode = new Node;
        newNode->cell = listString[i];
        newNode->next = nullptr;

        if (newRow->cell == nullptr) {
            newRow->cell = newNode;
        } else {
            currNode->next = newNode;
        }
        currNode = newNode;
    }

    if (table == nullptr) {
        table = newRow;
    } else {
        RowNode* currRow = table;
        while (currRow->nextRow != nullptr) {
            currRow = currRow->nextRow;
        }
        currRow->nextRow = newRow;
    }

    int fileIndex = 1;
    string filePath = tablePath + "/" + to_string(fileIndex) + ".csv";
    while (filesystem::exists(filePath)) {
        int lineCount = 0;
        ifstream file(filePath);
        if (!file.is_open()) {
            cerr << "Не удалось открыть файл " << filePath << " для чтения!\n";
            return nullptr;
        }
        string line;
        while (getline(file, line)) ++lineCount;

        if (lineCount < tuplesLimit + 1) { 
            break;
        }
        fileIndex++;
        filePath = tablePath + "/" + to_string(fileIndex) + ".csv";
    }

    if (!filesystem::exists(filePath)) {
        ofstream newFile(filePath);
        if (!newFile.is_open()) {
            cerr << "Не удалось создать файл " << filePath << " для записи!\n";
            return nullptr;
        }
		for (auto& [tableName, columns] : structure.items()){
			for (size_t i = 0; i < columns.size(); ++i) {
                newFile << columns[i];
                if (i < columns.size() - 1) {
                    newFile << ",";
                }
            }
			newFile << "\n";
			newFile.close();
		}
    }

    int numCols = 0;
    while (listString[numCols] != "") numCols++;
    appendRowToCSV(filePath, listString, numCols);
    responseStream << "Строка успешно добавлена!\n";
    return table;
}

RowNode* cartesianProduct(RowNode** tables, int countTables) {
    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;

    Node* lastHeaderCell = nullptr;

    for (int i = 0; i < countTables; ++i) {
        Node* headerCell = tables[i]->cell;

        while (headerCell != nullptr) {
            Node* newHeaderCell = new Node;
            newHeaderCell->name = headerCell->name;
            newHeaderCell->cell = headerCell->cell;
            newHeaderCell->tableName = headerCell->tableName;
            newHeaderCell->numColumn = headerCell->numColumn; 
            newHeaderCell->next = nullptr;

            if (headerRow->cell == nullptr) {
                headerRow->cell = newHeaderCell;
            } else {
                lastHeaderCell->next = newHeaderCell;
            }
            lastHeaderCell = newHeaderCell;
            headerCell = headerCell->next;
        }
    }

    RowNode* crossTable = nullptr;

    for (int i = 1; i < countTables; ++i) {
        RowNode* currentTable = tables[i];
        RowNode* newCrossTable = nullptr;
        RowNode* rowA = crossTable ? crossTable : tables[0]->nextRow;

        while (rowA != nullptr) {
            RowNode* rowB = currentTable->nextRow;

            while (rowB != nullptr) {
                RowNode* newRow = new RowNode;
                newRow->cell = nullptr;
                newRow->nextRow = nullptr;

                Node* currCellA = rowA->cell;
                Node* lastCell = nullptr;

                while (currCellA != nullptr) {
                    Node* newCell = new Node;
                    newCell->name = currCellA->name;
                    newCell->cell = currCellA->cell;
                    newCell->tableName = currCellA->tableName; 
                    newCell->numColumn = currCellA->numColumn;
                    newCell->next = nullptr;

                    if (newRow->cell == nullptr) {
                        newRow->cell = newCell;
                    } else {
                        lastCell->next = newCell;
                    }
                    lastCell = newCell;
                    currCellA = currCellA->next;
                }

                Node* currCellB = rowB->cell;
                while (currCellB != nullptr) {
                    Node* newCell = new Node;
                    newCell->name = currCellB->name;
                    newCell->cell = currCellB->cell;
                    newCell->tableName = currCellB->tableName; 
                    newCell->numColumn = currCellB->numColumn;
                    newCell->next = nullptr;

                    if (newRow->cell == nullptr) {
                        newRow->cell = newCell;
                    } else {
                        lastCell->next = newCell;
                    }
                    lastCell = newCell;
                    currCellB = currCellB->next;
                }

                if (newCrossTable == nullptr) {
                    newCrossTable = newRow;
                } else {
                    RowNode* lastRow = newCrossTable;
                    while (lastRow->nextRow != nullptr) {
                        lastRow = lastRow->nextRow;
                    }
                    lastRow->nextRow = newRow;
                }
                rowB = rowB->nextRow;
            }
            rowA = rowA->nextRow;
        }
        crossTable = newCrossTable;
    }

    headerRow->nextRow = crossTable;
    return headerRow;
}



RowNode* selectFromTable(RowNode* table, string columns[], int& columnCount, const string& tableName) {
    
    int columnIndexes[100];
    columnCount = 0; 

    bool selectAllColumns = false;
    for (int i = 0; columns[i] != ""; i++) {
        if (columns[i] == "*") {
            selectAllColumns = true;
            break;
        }
    }

    if (selectAllColumns || columns[0] == "") {
        Node* headerCell = table->cell;
        int columnIndex = 0;
        while (headerCell != nullptr) {
            columnIndexes[columnCount] = columnIndex;
            columnCount++;
            headerCell = headerCell->next;
            columnIndex++;
        }
    } else {
        Node* headerCell = table->cell;
        int columnIndex = 0;
        while (headerCell != nullptr) {
            for (int i = 0; columns[i] != ""; i++) {
                if (headerCell->name == columns[i]) {
                    columnIndexes[columnCount] = columnIndex; 
                    columnCount++;
                    break;
                }
            }
            headerCell = headerCell->next;
            columnIndex++;
        }
    }

    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;
    Node* lastHeaderCell = nullptr;

    Node* headerCell = table->cell;
    int columnIndex = 0;
    while (headerCell != nullptr) {
        for (int i = 0; i < columnCount; i++) {
            if (columnIndex == columnIndexes[i]) {
                Node* newHeaderCell = new Node;
                newHeaderCell->name = headerCell->name;
                newHeaderCell->cell = headerCell->cell;
                newHeaderCell->next = nullptr;

                if (headerRow->cell == nullptr) {
                    headerRow->cell = newHeaderCell;
                } else {
                    lastHeaderCell->next = newHeaderCell;
                }

                lastHeaderCell = newHeaderCell;
                break;
            }
        }
        headerCell = headerCell->next;
        columnIndex++;
    }

    RowNode* newTable = nullptr;
    RowNode* newTableTail = nullptr;

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        RowNode* newRow = new RowNode;
        newRow->nextRow = nullptr;

        Node* newCell = nullptr;
        Node* newCellTail = nullptr;

        Node* cell = currentRow->cell;
        int cellIndex = 0;

        while (cell != nullptr) {
            for (int i = 0; i < columnCount; i++) {
                if (cellIndex == columnIndexes[i]) {
                    Node* copiedCell = new Node;
                    copiedCell->name = cell->name;
                    copiedCell->cell = cell->cell;
                    copiedCell->next = nullptr;

                    if (newCell == nullptr) {
                        newCell = copiedCell;
                        newCellTail = copiedCell;
                    } else {
                        newCellTail->next = copiedCell;
                        newCellTail = newCellTail->next;
                    }
                }
            }
            cell = cell->next;
            cellIndex++;
        }

        if (newCell != nullptr) {
            newRow->cell = newCell;

            if (newTable == nullptr) {
                newTable = newRow;
                newTableTail = newRow;
            } else {
                newTableTail->nextRow = newRow;
                newTableTail = newTableTail->nextRow;
            }
        }

        currentRow = currentRow->nextRow;
    }

    headerRow->nextRow = newTable;

    addColumnNames(headerRow);
    addTableNames(headerRow, tableName);

    return headerRow;
}


Node* split(const string& input, const string& delimiter) {
    size_t start = 0;
    size_t end = input.find(delimiter);
    Node* head = nullptr;
    Node* tail = nullptr;

    while (end != string::npos) {
        string part = input.substr(start, end - start);

        part.erase(part.find_last_not_of(" ") + 1);
        part.erase(0, part.find_first_not_of(" "));

        Node* newNode = new Node;
        newNode->cell = part;
        newNode->next = nullptr;

        if (head == nullptr) {
            head = newNode;
        } else {
            tail->next = newNode;
        }
        tail = newNode;

        start = end + delimiter.length();
        end = input.find(delimiter, start);
    }

    if (start < input.length()) {
        string part = input.substr(start);

        part.erase(part.find_last_not_of(" ") + 1);
        part.erase(0, part.find_first_not_of(" "));

        Node* newNode = new Node;
        newNode->cell = part;
        newNode->next = nullptr;

        if (head == nullptr) {
            head = newNode;
        } else {
            tail->next = newNode;
        }
    }

    return head;
}

bool checkConditions(RowNode* baseDataLine, const string& filter) {
    Node* orSplitting = split(filter, "OR");

    while (orSplitting != nullptr) {
        string orSplit = orSplitting->cell;
        Node* andSplit = split(orSplit, "AND");
        bool isAnd = true;

        while (andSplit != nullptr) {
            Node* spaceSplit = split(andSplit->cell, " ");
            if (spaceSplit == nullptr || spaceSplit->next == nullptr || spaceSplit->next->next == nullptr) {
                return false;
            }

            string colNameLeftWithTable = spaceSplit->cell;
            string rightValue = spaceSplit->next->next->cell;

            bool isStringConstant = (rightValue[0] == '\'');
            if (isStringConstant) {
                rightValue = rightValue.substr(1, rightValue.size() - 2);
            }

            size_t dotPos = colNameLeftWithTable.find('.');
            if (dotPos == string::npos) {
                return false;
            }
            string tableNameLeft = colNameLeftWithTable.substr(0, dotPos);
            string colNameLeft = colNameLeftWithTable.substr(dotPos + 1);

            Node* leftValue = baseDataLine->cell;
            bool foundLeft = false;
            while (leftValue != nullptr) {
                if (leftValue->name == colNameLeft && leftValue->tableName == tableNameLeft) {
                    foundLeft = true;
                    break;
                }
                leftValue = leftValue->next;
            }
            
            if (!foundLeft) {
                isAnd = false;
                break;
            }

            if (isStringConstant) {
                if (leftValue->cell != rightValue) {
                    isAnd = false;
                    break;
                }
            } else {
                size_t dotPosRight = rightValue.find('.');
                if (dotPosRight == string::npos) {
                    return false;
                }

                string tableNameRight = rightValue.substr(0, dotPosRight);
                string colNameRight = rightValue.substr(dotPosRight + 1);

                Node* rightValueNode = baseDataLine->cell;
                bool foundRight = false;
                while (rightValueNode != nullptr) {
                    if (rightValueNode->name == colNameRight && rightValueNode->tableName == tableNameRight) {
                        foundRight = true;
                        break;
                    }
                    rightValueNode = rightValueNode->next;
                }

                if (!foundRight || leftValue->cell != rightValueNode->cell) {
                    isAnd = false;
                    break;
                }
            }

            andSplit = andSplit->next;
        }

        if (isAnd) {
            return true;
        }

        orSplitting = orSplitting->next;
    }

    return false;
}

RowNode* selectFiltration(RowNode* table, const string& filter) {
    if (table == nullptr) {
        return nullptr;
    }

    RowNode* newTable = new RowNode;
    newTable->name = table->name;
    newTable->cell = nullptr;
    newTable->nextRow = nullptr;
    RowNode* lastRow = newTable;

    Node* currentCell = table->cell;
    Node* lastCell = nullptr;
    while (currentCell != nullptr) {
        Node* newCell = new Node;
        newCell->numColumn = currentCell->numColumn;
        newCell->name = currentCell->name;
        newCell->tableName = currentCell->tableName;
        newCell->cell = currentCell->cell;
        newCell->next = nullptr;

        if (newTable->cell == nullptr) {
            newTable->cell = newCell;
        } else {
            lastCell->next = newCell;
        }
        lastCell = newCell;

        currentCell = currentCell->next;
    }

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        if (checkConditions(currentRow, filter)) {
            RowNode* newRow = new RowNode;
            newRow->name = currentRow->name;
            newRow->cell = nullptr;
            newRow->nextRow = nullptr;

            Node* currentCell = currentRow->cell;
            Node* lastCell = nullptr;
            while (currentCell != nullptr) {
                Node* newCell = new Node;
                newCell->numColumn = currentCell->numColumn;
                newCell->name = currentCell->name;
                newCell->tableName = currentCell->tableName;
                newCell->cell = currentCell->cell;
                newCell->next = nullptr;

                if (newRow->cell == nullptr) {
                    newRow->cell = newCell;
                } else {
                    lastCell->next = newCell;
                }
                lastCell = newCell;

                currentCell = currentCell->next;
            }

            lastRow->nextRow = newRow;
            lastRow = newRow;
        }
        currentRow = currentRow->nextRow;
    }

    return newTable;
}

RowNode* deleteFrom(RowNode* table, const string& filter) {
    if (table == nullptr) {
        return nullptr;
    }

    RowNode* newTable = new RowNode;
    newTable->name = table->name;
    newTable->cell = nullptr;
    newTable->nextRow = nullptr;
    RowNode* lastRow = newTable;

    Node* currentCell = table->cell;
    Node* lastCell = nullptr;
    while (currentCell != nullptr) {
        Node* newCell = new Node;
        newCell->numColumn = currentCell->numColumn;
        newCell->name = currentCell->name;
        newCell->tableName = currentCell->tableName;
        newCell->cell = currentCell->cell;
        newCell->next = nullptr;

        if (newTable->cell == nullptr) {
            newTable->cell = newCell;
        } else {
            lastCell->next = newCell;
        }
        lastCell = newCell;

        currentCell = currentCell->next;
    }

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        if (checkConditions(currentRow, filter) == false) {
            RowNode* newRow = new RowNode;
            newRow->name = currentRow->name;
            newRow->cell = nullptr;
            newRow->nextRow = nullptr;

            Node* currentCell = currentRow->cell;
            Node* lastCell = nullptr;
            while (currentCell != nullptr) {
                Node* newCell = new Node;
                newCell->numColumn = currentCell->numColumn;
                newCell->name = currentCell->name;
                newCell->tableName = currentCell->tableName;
                newCell->cell = currentCell->cell;
                newCell->next = nullptr;

                if (newRow->cell == nullptr) {
                    newRow->cell = newCell;
                } else {
                    lastCell->next = newCell;
                }
                lastCell = newCell;

                currentCell = currentCell->next;
            }

            lastRow->nextRow = newRow;
            lastRow = newRow;
        }
        currentRow = currentRow->nextRow;
    }

    return newTable;
}