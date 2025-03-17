#include <iostream>
#include <thread>
#include <cstring>
#include <map>

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "ChordNode.h"

#define BUFFER_SIZE 1024

bool server_running = true;
std::map<Identifier, std::string> keys;
std::mutex keys_mutex;

bool request_filename(const std::string &filename, const ChordNode &remote_node) {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        std::cerr << "<ChordNode::predRPC> socket failed " << strerror(errno) << std::endl;
        return false;
    }

    const int port = static_cast<int>(strtol(remote_node.getAddress().c_str(), nullptr, 10));

    sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
        std::cerr << "<request_filename> connect failed " << strerror(errno) << std::endl;
        close(socket_fd);
        return false;
    }

    std::string command = "rqfn";
    send(socket_fd, command.c_str(), command.length(), 0);
    uint32_t filename_size = htonl(filename.length());
    send(socket_fd, &filename_size, sizeof(uint32_t), 0);
    send(socket_fd, filename.c_str(), filename.length(), 0);

    char response[BUFFER_SIZE];
    ssize_t bytes_received = recv(socket_fd, response, 1, 0);
    if (bytes_received == -1) {
        std::cerr << "<request_filename> recv failed" << std::endl;
        close(socket_fd);
        return false;
    }
    response[1] = '\0';

    close(socket_fd);
    return response[0];
}

bool send_filename(const std::string &filename, const ChordNode &remote_node) {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        std::cerr << "<ChordNode::predRPC> socket failed " << strerror(errno) << std::endl;
        return false;
    }

    const int port = static_cast<int>(strtol(remote_node.getAddress().c_str(), nullptr, 10));

    sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
        std::cerr << "<request_filename> connect failed " << strerror(errno) << std::endl;
        close(socket_fd);
        return false;
    }

    std::string command = "infn";
    send(socket_fd, command.c_str(), command.length(), 0);
    uint32_t filename_size = htonl(filename.length());
    send(socket_fd, &filename_size, sizeof(uint32_t), 0);
    send(socket_fd, filename.c_str(), filename.length(), 0);

    char response[BUFFER_SIZE];
    ssize_t bytes_received = recv(socket_fd, response, 1, 0);
    if (bytes_received == -1) {
        std::cerr << "<request_filename> recv failed" << std::endl;
        close(socket_fd);
        return false;
    }
    response[1] = '\0';

    close(socket_fd);
    return response[0];
}

void handleClient(int client_sk, ChordNode &chord_node) {
    char buffer[BUFFER_SIZE] = {};

    ssize_t bytes_received = recv(client_sk, buffer, 4, 0);
    if (bytes_received <= 0) {
        std::cerr << "<handleClient> Failed to receive command: " << strerror(errno) << std::endl;
        close(client_sk);
        return;
    }

    buffer[4] = '\0';
    std::string command(buffer);

    if (command == "gNod") {
        std::vector<char> serialized_node = chord_node.serialize();
        size_t total_sent = 0;
        while (total_sent < serialized_node.size()) {
            size_t chunk_size = std::min(BUFFER_SIZE, static_cast<int>(serialized_node.size() - total_sent));
            ssize_t bytes_sent = send(client_sk, serialized_node.data() + total_sent, chunk_size, 0);
            if (bytes_sent == -1) {
                std::cerr << "<handleClient> Failed to send serialized data: " << strerror(errno) << std::endl;
                close(client_sk);
                return;
            }
            total_sent += bytes_sent;
        }
    } else if (command == "pRPC") {
        char pred_buffer[BUFFER_SIZE] = {};
        uint32_t address_length_net;
        ssize_t len_bytes = recv(client_sk, &address_length_net, sizeof(address_length_net), 0);
        if (len_bytes != sizeof(address_length_net)) {
            std::cerr << "<handleClient> Failed to receive address length: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }
        uint32_t address_length = ntohl(address_length_net);

        ssize_t pred_bytes = recv(client_sk, pred_buffer, address_length, 0);
        if (pred_bytes <= 0) {
            std::cerr << "<handleClient> Failed to receive pred_RPC data: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }

        pred_buffer[address_length] = '\0';
        std::string new_address(pred_buffer);
        chord_node.setPred_address(new_address);

    } else if (command == "fRPC") {
        Identifier s_id;
        ssize_t id_bytes = recv(client_sk, s_id.begin(), Identifier::size(), 0);
        if (id_bytes != Identifier::size()) {
            std::cerr << "<handleClient> Failed to receive Identifier: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }

        uint32_t address_length_net;
        ssize_t len_bytes = recv(client_sk, &address_length_net, sizeof(address_length_net), 0);
        if (len_bytes != sizeof(address_length_net)) {
            std::cerr << "<handleClient> Failed to receive address length: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }
        uint32_t address_length = ntohl(address_length_net);

        char address_buffer[BUFFER_SIZE] = {};
        ssize_t address_bytes = recv(client_sk, address_buffer, address_length, 0);
        if (address_bytes <= 0) {
            std::cerr << "<handleClient> Failed to receive address: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }
        address_buffer[address_length] = '\0';
        std::string s_address(address_buffer);

        int i_data;
        ssize_t i_bytes = recv(client_sk, &i_data, sizeof(i_data), 0);
        if (i_bytes != sizeof(i_data)) {
            std::cerr << "<handleClient> Failed to receive pointer data: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }

        chord_node.setFinger(i_data, s_id, s_address);
    } else if (command == "infn") {
        uint32_t filename_size;
        ssize_t len_bytes = recv(client_sk, &filename_size, sizeof(filename_size), 0);
        if (len_bytes == -1) {
            std::cerr << "<handleClient> Failed to receive filename length: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }
        filename_size = ntohl(filename_size);

        char filename[BUFFER_SIZE];
        len_bytes = recv(client_sk, filename, filename_size, 0);
        if (len_bytes == -1) {
            std::cerr << "<handleClient> Failed to receive filename: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }

        filename[filename_size] = '\0';
        std::string filename_str(filename);

        Identifier id(filename_str);
        {
            std::lock_guard lock(keys_mutex);
            keys.insert({id, filename_str});
        }

        uint8_t var = 1;
        send(client_sk, &var, sizeof(var), 0);
    } else if (command == "rqfn") {
        uint32_t filename_size;
        ssize_t len_bytes = recv(client_sk, &filename_size, sizeof(filename_size), 0);
        if (len_bytes == -1) {
            std::cerr << "<handleClient> Failed to receive filename length: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }
        filename_size = ntohl(filename_size);

        char filename[BUFFER_SIZE];
        len_bytes = recv(client_sk, filename, filename_size, 0);
        if (len_bytes == -1) {
            std::cerr << "<handleClient> Failed to receive filename: " << strerror(errno) << std::endl;
            close(client_sk);
            return;
        }

        filename[filename_size] = '\0';
        std::string filename_str(filename);

        uint8_t found = 0;
        {
            std::lock_guard lock(keys_mutex);
            for (auto &key : keys)
                if (key.second == filename_str) {
                    found = 1;
                    break;
                }
        }

        send(client_sk, &found, sizeof(found), 0);
    }

    close(client_sk);
}


void server_thread(int port, ChordNode &chord_node) {
    int server_sk;

    if ((server_sk = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        std::cerr << "<Server thread> socket failed: " << strerror(errno) << std::endl;
        return;
    }

    int opt = 1;
    if (setsockopt(server_sk, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        std::cerr << "<Server thread> setsockopt failed: " << strerror(errno) << std::endl;
        close(server_sk);
        return;
    }

    sockaddr_in server_address = {};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port);
    server_address.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_sk, reinterpret_cast<sockaddr*>(&server_address), sizeof(server_address)) == -1) {
        std::cerr << "<Server thread> bind failed: " << strerror(errno) << std::endl;
        close(server_sk);
        return;
    }

    if (listen(server_sk, 5) == -1) {
        std::cerr << "<Server thread> listen failed: " << strerror(errno) << std::endl;
        close(server_sk);
        return;
    }

    while (server_running) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(server_sk, &readfds);

        timeval timeout = {1, 0};
        int activity = select(server_sk + 1, &readfds, nullptr, nullptr, &timeout);

        if (activity == -1) {
            std::cerr << "<Server thread> select failed: " << strerror(errno) << std::endl;
            break;
        }
        if (activity == 0) {
            continue;
        }

        sockaddr_in client_addr{};
        socklen_t client_addr_size = sizeof(client_addr);
        int client_sk = accept(server_sk, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_size);

        if (client_sk == -1) {
            std::cerr << "<Server thread> accept failed: " << strerror(errno) << std::endl;
        }

        std::thread client_th(handleClient, client_sk, std::ref(chord_node));
        client_th.detach();
    }
}

int main(int argc, const char *argv[]) {
    if (argc != 3 and argc != 2) {
        std::cerr << "Usage: " << argv[0] << " port <port_in_the_network>" << std::endl;
        return 1;
    }

    const std::string port_str = argv[1];
    ChordNode node(port_str);

    if (argc == 2) //first node in the network
        node.join(port_str);
    else {
        const std::string port_ith_str = argv[2];//ith = in the network
        node.join(port_ith_str);
    }

    std::cout << "Client ready;" << std::endl;

    const int port = static_cast<int>(strtol(argv[1], nullptr, 10));

    std::thread server(server_thread, port, std::ref(node));

    while (true) {
        std::string input;
        std::cin >> input;

        if (input == "exit") {
            server_running = false;
            server.join();
            return 0;
        }

        if (input == "request_file_location") {
            std::cout << "filename: " ;
            std::string filename;
            std::cin >> filename;

            Identifier id(filename);
            ChordNode key_node = node.find_successor(id);
            std::cout << "requested key is at the node with id:" << std::endl;
            key_node.printId();
            std::cout << "at address: ";
            key_node.printAddress();

        } else if (input == "print_node_info") {
            node.printNodeInfo();
        } else if (input == "print_pred") {
            node.printPred();
        } else if (input == "print_succ") {
            node.printSucc();
        } else if (input == "print_id") {
            node.printId();
        } else if (input == "request_file") {
            std::cout << "filename: " ;
            std::string filename;
            std::cin >> filename;
            Identifier id(filename);
            ChordNode key_node = node.find_successor(id);
            if (key_node.getAddress() == node.getAddress()) {
                std::cout << "The key should be stored in the local node. Searching up..." << std::endl;
                bool found = false;
                {
                    std::lock_guard lock(keys_mutex);
                    for (const auto &key : keys)
                        if (key.second == filename)
                            found = true;
                }
                if (found)
                    std::cout << "The requested filename exists." << std::endl;
                else
                    std::cout << "The requested filename does not exist." << std::endl;
            } else {
                std::cout << "The key should be stored in the node with the id:\n";
                key_node.printId();
                std::cout << "at address: ";
                key_node.printAddress();
                std::cout << "Searching up..." << std::endl;
                bool result = request_filename(filename, key_node);
                if (result == false)
                    std::cout << "The requested filename does not exist." << std::endl;
                else
                    std::cout << "The requested filename exists." << std::endl;
            }
        } else if (input == "insert_file") {
            std::cout << "filename: ";
            std::string filename;
            std::cin >> filename;
            Identifier id(filename);
            ChordNode key_node = node.find_successor(id);
            if (key_node.getAddress() == node.getAddress()) {
                std::cout << "The key should be stored in the local node. Storing..." << std::endl;
                {
                    std::lock_guard lock(keys_mutex);
                    keys.insert({id, filename});
                }
                std::cout << "The key-value pair was stored" << std::endl;
            } else {
                std::cout << "The key-value should be stored in the node with the id:\n";
                key_node.printId();
                std::cout << "at address: ";
                key_node.printAddress();
                std::cout << "Sending the key-value pair." << std::endl;
                bool result = send_filename(filename, key_node);
                if (result == false)
                    std::cout << "The key-value was not saved successfully." << std::endl;
                else
                    std::cout << "The key-value was saved successfully." << std::endl;
            }
        } else {
            std::cout << "unknown command" << std::endl;
        }
    }
}