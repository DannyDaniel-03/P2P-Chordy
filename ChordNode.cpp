//
// Created by daniel on 12/28/24.
//

#include "ChordNode.h"

#include <iostream>
#include <unistd.h>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define BUFFER_SIZE 1024
//smth
bool ChordNode::inInterval(const Identifier &x, const Identifier &a, const Identifier &b, IntervalType interval_type) {
    bool lowerBound = interval_type == IntervalType::OO or interval_type == IntervalType::OC ? a < x : a <= x;
    bool upperBound = interval_type == IntervalType::OO or interval_type == IntervalType::CO ? x < b : x <= b;

    if (a < b)
        return lowerBound and upperBound;

    return lowerBound or upperBound;
}

ChordNode::ChordNode(const std::vector<char> &data) {
    long offset = 0;

    auto deserializeString = [&data, &offset] {
        char length;
        std::memcpy(&length, &data[offset], sizeof(length));
        offset += sizeof(length);

        std::string result(data.begin() + offset, data.begin() + offset + length);
        offset += length;

        return result;
    };

    address = deserializeString();
    pred_address = deserializeString();
    id.set(data, offset);
    offset += Identifier::size();

    int m = Identifier::length();
    fingerTable.resize(m);
    for (int i = 0; i < m; i++) {
        fingerTable[i].start.set(data, offset);
        offset += Identifier::size();
        fingerTable[i].node_id.set(data, offset);
        offset += Identifier::size();
        fingerTable[i].node_address = deserializeString();
    }
}

ChordNode ChordNode::getNode(const std::string &address) const {
    if (address == this->address)
        return *this;

    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        std::cerr << "<ChordNode::getNode> socket failed " << strerror(errno) << std::endl;
        return *this;
    }

    const int port = static_cast<int>(strtol(address.c_str(), nullptr, 10));

    sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
        std::cerr << "<ChordNode::getNode> connect failed " << strerror(errno) << std::endl;
        close(socket_fd);
        return *this;
    }

    std::string command = "gNod";
    if (send(socket_fd, command.c_str(), command.length(), 0) == -1) {
        std::cerr << "<ChordNode::getNode> send failed " << strerror(errno) << std::endl;
        close(socket_fd);
        return *this;
    }

    std::vector<char> serialized_node;
    char buffer[BUFFER_SIZE];
    ssize_t bytes;
    while ((bytes = recv(socket_fd, buffer, BUFFER_SIZE, 0)) > 0) {
        serialized_node.insert(serialized_node.end(), buffer, buffer + bytes);
    }

    close(socket_fd);

    ChordNode node(serialized_node);
    return node;
}

void ChordNode::pred_RPC(const std::string &remote_address, const std::string &new_address) const {
    if (remote_address == this->address)
        return;

    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        std::cerr << "<ChordNode::predRPC> socket failed " << strerror(errno) << std::endl;
        return;
    }

    const int port = static_cast<int>(strtol(remote_address.c_str(), nullptr, 10));

    sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
        std::cerr << "<ChordNode::predRPC> connect failed " << strerror(errno) << std::endl;
        close(socket_fd);
        return;
    }

    std::string command = "pRPC";
    send(socket_fd, command.c_str(), command.length(), 0);
    uint32_t address_length = htonl(new_address.length());
    send(socket_fd, &address_length, sizeof(address_length), 0);
    send(socket_fd, new_address.c_str(), new_address.length(), 0);
    close(socket_fd);
}

void ChordNode::finger_RPC(const Identifier &s_id, const std::string &s_address, int i) const {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        std::cerr << "<ChordNode::finger_RPC> socket failed " << strerror(errno) << std::endl;
        return;
    }

    const int port = static_cast<int>(strtol(address.c_str(), nullptr, 10));

    sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
        std::cerr << "<ChordNode::finger_RPC> connect failed " << strerror(errno) << std::endl;
        close(socket_fd);
        return;
    }

    std::string command = "fRPC";
    send(socket_fd, command.c_str(), command.length(), 0);
    send(socket_fd, s_id.begin(), Identifier::size(), 0);
    uint32_t address_length = htonl(s_address.length());
    send(socket_fd, &address_length, sizeof(address_length), 0);
    send(socket_fd, s_address.c_str(), s_address.length(), 0);
    send(socket_fd, &i, sizeof(i), 0);

    close(socket_fd);
}

ChordNode::ChordNode() = default;

ChordNode::ChordNode(const std::string &address) : address(address), id(address) {
    int m = Identifier::length();
    fingerTable.resize(m);
    for (int i = 0; i < m; i++) {
        Identifier pow_of_two(i);
        fingerTable[i].start = this->id + pow_of_two;
    }
}

ChordNode::ChordNode(const ChordNode &other) {
    address = other.address;
    pred_address = other.pred_address;
    fingerTable = other.fingerTable;
    id = other.id;
}

std::vector<char> ChordNode::serialize() {
    std::lock_guard lock(mtx);
    std::vector<char> buffer;
    buffer.push_back(static_cast<char>(address.length()));
    buffer.insert(buffer.end(), address.cbegin(), address.cend());
    buffer.push_back(static_cast<char>(pred_address.length()));
    buffer.insert(buffer.end(), pred_address.cbegin(), pred_address.cend());
    buffer.insert(buffer.end(), id.begin(), id.end());

    for (int i = 0; i < Identifier::length(); i++) {
        buffer.insert(buffer.end(), fingerTable[i].start.begin(), fingerTable[i].start.end());
        buffer.insert(buffer.end(), fingerTable[i].node_id.begin(), fingerTable[i].node_id.end());
        buffer.push_back(static_cast<char>(fingerTable[i].node_address.length()));
        buffer.insert(buffer.end(), fingerTable[i].node_address.cbegin(), fingerTable[i].node_address.cend());
    }

    return buffer;
}

void ChordNode::printId() const {
    id.print();
}

void ChordNode::printAddress() const {
    std::cout << address << std::endl;
}

std::string ChordNode::getAddress() const {
    return address;
}

void ChordNode::setPred_address(const std::string &new_pred_address) {
    std::lock_guard lock(mtx);
    pred_address = new_pred_address;
}

void ChordNode::setFinger(int index, const Identifier &new_node_id, const std::string &new_node_address) {
    std::lock_guard lock(mtx);
    fingerTable[index].node_address = new_node_address;
    fingerTable[index].node_id = new_node_id;
}

std::string ChordNode::closest_preceding_finger(const Identifier &id) const {
    int m = Identifier::length();
    for (int i = m - 1; i > 0; i--) {
        if (inInterval(fingerTable[i].node_id, this->id, id, IntervalType::OO))
            return fingerTable[i].node_address;
    }
    return address;
}

ChordNode ChordNode::find_predecessor(const Identifier &id) const {
    ChordNode n_prime = *this;
     while(!inInterval(id, n_prime.id, n_prime.fingerTable[0].node_id, IntervalType::OC))
        n_prime = getNode(closest_preceding_finger(id));
    return n_prime;
}

ChordNode ChordNode::find_successor(const Identifier &id) const {
    ChordNode n_prime = find_predecessor(id);
    return getNode(n_prime.fingerTable[0].node_address);
}

void ChordNode::update_finger_table(const Identifier &s_id, const std::string &s_address, int i, const std::string& original_address) {
    bool shouldUpdate = false;
    {
        std::lock_guard lock(mtx);
        if (inInterval(s_id, this->id, fingerTable[i].node_id, IntervalType::CO))
            shouldUpdate = true;
    }
    if (shouldUpdate) {
        if (original_address == address) return;
        finger_RPC(s_id, s_address, i); // This sends the update via RPC.
        if (original_address == pred_address) return;
        ChordNode p = getNode(pred_address);
        p.update_finger_table(s_id, s_address, i, original_address);
    }
}

void ChordNode::update_others() const {
    int m = Identifier::length();
    for (int i = 0; i < m; i++) {
        Identifier pow_of_two(i);
        ChordNode p = find_predecessor(this->id - pow_of_two);
        p.update_finger_table(this->id, this->address, i, address);
    }
}


void ChordNode::init_finger_table(const std::string &n_prime_address) {
    ChordNode n_prime = getNode(n_prime_address);
    ChordNode successor = n_prime.find_successor(fingerTable[0].start);
    {
        std::lock_guard lock(mtx);
        fingerTable[0].node_address = successor.address;
        fingerTable[0].node_id = successor.id;
        pred_address = successor.pred_address;
    }
    //successor.pred_address = address;//<DONE>!!!Make sure to send this change to the actual node(aka implement the RPC)
    pred_RPC(successor.address, address);//basically 'successor.pred_address = address', but on the actual node
    int m = Identifier::length();
    for (int i = 0; i < m - 1; i++) {
        {
            std::lock_guard lock(mtx);
            if (inInterval(fingerTable[i+1].start, this->id, fingerTable[i].node_id, IntervalType::CO)) {
                fingerTable[i+1].node_id = fingerTable[i].node_id;
                fingerTable[i+1].node_address = fingerTable[i].node_address;
                continue;
            }
        }
        successor = find_successor(fingerTable[i+1].node_id);
        {
            std::lock_guard lock(mtx);
            fingerTable[i+1].node_id = successor.id;
            fingerTable[i+1].node_address = successor.address;
        }
    }
}

void ChordNode::join(const std::string &n_prime_address) {
    if (n_prime_address == address) {//self join means first node in the network
        for (auto &finger : fingerTable) {
            finger.node_id = id;
            finger.node_address = address;
        }
        pred_address = address;
    } else {
        init_finger_table(n_prime_address);
        update_others();
    }
}

bool ChordNode::operator==(const Identifier &other) const {
    return this->id == other;
}

bool ChordNode::operator!=(const Identifier &other) const {
    return !(*this == other);
}

bool ChordNode::operator<(const Identifier &other) const {
    return this->id < other;
}

bool ChordNode::operator<=(const Identifier &other) const {
    return *this < other or *this == other;
}

bool ChordNode::operator>(const Identifier &other) const {
    return !(*this <= other);
}

bool ChordNode::operator>=(const Identifier &other) const {
    return !(*this < other);
}

bool ChordNode::operator==(const ChordNode &other) const {
    return this->id == other.id;
}

bool ChordNode::operator!=(const ChordNode &other) const {
    return !(*this == other);
}

bool ChordNode::operator<(const ChordNode &other) const {
    return this->id < other.id;
}

bool ChordNode::operator<=(const ChordNode &other) const {
    return *this == other or *this < other;
}

bool ChordNode::operator>(const ChordNode &other) const {
    return !(*this<=other);
}

bool ChordNode::operator>=(const ChordNode &other) const {
    return !(*this < other);
}

ChordNode& ChordNode::operator=(const ChordNode &other) {
    address = other.address;
    id = other.id;
    pred_address = other.pred_address;
    fingerTable = other.fingerTable;
    return *this;
}

void ChordNode::printBytes() const {
    for (const auto &byte : address)
        std::cout << static_cast<int>(byte) << ' ';
    for (const auto &byte : pred_address)
        std::cout << static_cast<int>(byte) << ' ';
    id.printBytes();
    for (const auto &finger : fingerTable) {
        finger.start.printBytes();
        finger.node_id.printBytes();
        for (const auto &byte : finger.node_address)
            std::cout << static_cast<int>(byte) << ' ';
    }
}

void ChordNode::printNodeInfo() const {
    std::cout << "The node address: " << address << std::endl;
    std::cout << "The predecessor node address: " << pred_address << std::endl;
    std::cout << "The node identifier: "; id.print();
    std::cout << "The finger table:" << std::endl;
    for (const auto &finger : fingerTable) {
        std::cout << "Start: "; finger.start.print();
        std::cout << "Node id: "; finger.node_id.print();
        std::cout << "Node address: " << finger.node_address << std::endl;
        std::cout << std::endl;
    }
}

void ChordNode::printPred() const {
    std::cout << "Predecessor node address: " << pred_address << std::endl;
}

void ChordNode::printSucc() const {
    std::cout << "Successor node address: " << fingerTable[0].node_address << std::endl;
    std::cout << "Successor node id:\n"; fingerTable[0].node_id.print();
}

void ChordNode::printNodeId() const {
    std::cout << "Node id:\n"; id.print();
}
