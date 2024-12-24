#pragma once

#include <string>
using namespace std;

struct Node {
	int numColumn;
	string name;
    string tableName;
	string cell;
	Node* next;
};

struct RowNode {
	string name;
	Node* cell;
	RowNode* nextRow;
};