#include "header.h"

#include <iostream>
#include <unistd.h> // для функций close
#include <string.h>
#include <arpa/inet.h> // для функций работы с сетевыми адресами(inet_addr, htons)
#include <sys/socket.h> // для работы с сокетами
#include <netinet/in.h> // для структуры sockaddr_in
#include <fstream> 
#include <sstream> // для строковых потоков(ostringstream)
#include <thread> // потоки
#include <csignal> // обработка сигналов(SIGINT для завершения работы сервера)

using namespace std;

int serverSocket;
bool isRunning = true;

void handleInterrupt(int signal) {
    if (signal == SIGINT) {
        isRunning = false;
        close(serverSocket); // прерывание вызова accept
    }
}

void processClientConnection(int clientSocket, ostringstream& responseStream) {
    char buffer[1024];
    int bytesReceived;

    while (true) {
        memset(buffer, 0, sizeof(buffer)); // очистка буфера

        bytesReceived = recv(clientSocket, buffer, sizeof(buffer) - 1, 0); // получение данных от клиента
        if (bytesReceived < 0) {
            cerr << "Ошибка при получении данных от клиента!" << endl;
            break;
        }

        if (bytesReceived == 0) {
            cout << "Клиент отключился!" << endl;
            break;
        }

        string clientMessage(buffer); // данные из буфера в строку

        // очистка потока перед новым выводом
        responseStream.str("");   
        responseStream.clear();

        dbmsQueries(clientMessage, responseStream);

        string response = responseStream.str();
        cout << "Отправка ответа клиенту: " << response << endl;

        int bytesSent = send(clientSocket, response.c_str(), response.size(), 0);
        if (bytesSent < 0) {
            cerr << "Ошибка при отправке данных клиенту!" << endl;
            break;
        }
    }

    close(clientSocket);
}

int main() {
    int clientSocket;
    struct sockaddr_in serverAddress, clientAddress;
    socklen_t clientAddressLen = sizeof(clientAddress);

    // создание серверного сокета
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        cerr << "Не удалось создать серверный сокет!" << endl;
        return -1;
    }

    // настройка адреса и порта сервера
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(7432); // преобразование порта в сетевой порядок байтов
    serverAddress.sin_addr.s_addr = inet_addr("127.0.0.1"); // преобразование адреса в числовой формат

    int reuseAddrOpt = 5;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &reuseAddrOpt, sizeof(reuseAddrOpt)) < 0) {
        perror("Не удалось установить SO_REUSEADDR!");
        close(serverSocket);
        return -1;
    }

    // привязка сокета к адресу
    if (bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("Не удалось привязать сокет");
        close(serverSocket);
        return -1;
    }

    // прослушивание входящих соединений
    if (listen(serverSocket, 1) < 0) {
        cerr << "Не удалось начать прослушивание порта!" << endl;
        close(serverSocket);
        return -1;
    }

    ostringstream responseStream; // поток ответов клиенту

    cout << "Ожидание подключения клиентов (порт 7432)..." << endl;

    signal(SIGINT, handleInterrupt);

    while (isRunning) {
        // ожидание подключения клиента
        clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddress, &clientAddressLen); // ожидает входящее соединение
        if (clientSocket < 0) {
            if (!isRunning) break;
            cerr << "Ошибка при подключении клиента!" << endl;
            continue;
        }

        cout << "Клиент подключился!" << endl;

        // cоздание нового потока для каждого клиента
        thread clientThread(processClientConnection, clientSocket, ref(responseStream));
        clientThread.detach(); // поток будет работать асинхронно(независимо от основного потока)
    }                          // потоки обрабатывают клиентов параллельно

    close(serverSocket);
    cout << endl << "Сервер завершил работу!" << endl;
    return 0;
}