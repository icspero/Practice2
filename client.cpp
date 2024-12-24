#include <iostream>
#include <unistd.h> // для функций close
#include <string.h>
#include <arpa/inet.h> // для функций работы с сетевыми адресами(inet_addr, htons)
#include <sys/socket.h> // для работы с сокетами
#include <netinet/in.h> // для структуры sockaddr_in

using namespace std;

#define MAX_BUFFER_SIZE 1024 // Размер буфера для получения данных

int main() {
    int socketDescriptor;  // дескриптор сокета(идентификатор)
    struct sockaddr_in serverAddress;  // структура для хранения адреса сервера
    char recvBuffer[MAX_BUFFER_SIZE];  // буфер для данных, получаемых от сервера
    string userInput;       // ввод пользователя

    // создание tcp сокета
    socketDescriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (socketDescriptor == -1) {
        cerr << "Не удалось создать сокет!" << endl;
        return 1;
    }

    // настройка параметров подключения к серверу
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(7432); // преобразование порта в сетевой порядок байтов
    serverAddress.sin_addr.s_addr = inet_addr("127.0.0.1"); // преобразование адреса в числовой формат

    if (connect(socketDescriptor, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) == -1) {
        cerr << "Ошибка при подключении к серверу!" << endl;
        close(socketDescriptor);
        return 1;
    }

    cout << "Соединение с сервером установлено!" << endl;

    while (true) {
        cout << "Введите команду: ";
        getline(cin, userInput);

        if (userInput == "exit") {
            cout << "Завершаю работу клиента..." << endl;
            break;
        }

        // отправка данных на сервер
        ssize_t bytesSent = send(socketDescriptor, userInput.c_str(), userInput.length(), 0);
        if (bytesSent == -1) {
            cerr << "Ошибка при отправке данных на сервер!" << endl;
            break;
        }

        // ожидание ответа от сервера
        string response = "";
        ssize_t bytesReceived;

        while (true) {
            memset(recvBuffer, 0, sizeof(recvBuffer)); // очищаем буфер перед приемом данных

            // Получение данных от сервера
            bytesReceived = recv(socketDescriptor, recvBuffer, sizeof(recvBuffer) - 1, 0);
            if (bytesReceived == -1) {
                cerr << "Ошибка при получении данных от сервера!" << endl;
                break;
            }

            if (bytesReceived == 0) {
                cout << "Сервер закрыл соединение!" << endl;
                break;
            }

            // Завершаем строку корректно
            recvBuffer[bytesReceived] = '\0';

            // Добавляем полученные данные к общей строке
            response.append(recvBuffer);

            // Проверка, если сервер закончил отправлять данные
            // Вы можете использовать логическую проверку или просто размер данных,
            // если это известно заранее (например, по размеру ответа или специальному маркеру)
            // Здесь условие нужно адаптировать под ваш случай.
            if (bytesReceived < sizeof(recvBuffer) - 1) {
                break;  // Если данных меньше чем размер буфера, предполагаем, что ответ завершен
            }
        }

        // Выводим окончательный ответ
        cout << "Ответ от сервера: " << response << endl;
    }

    close(socketDescriptor);
    return 0;
}
