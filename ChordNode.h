#ifndef CHORDNODE_H
#define CHORDNODE_H

#include "Identifier.h"

#include <mutex>

struct Finger;

class ChordNode {
    std::string address, pred_address;
    Identifier id;
    std::vector<Finger> fingerTable;

    std::mutex mtx;

    enum class IntervalType {OO, CO, OC, CC};
    static bool inInterval(const Identifier& x, const Identifier& a, const Identifier& b, IntervalType interval_type);

    explicit ChordNode(const std::vector<char>& data);//used for deserialization
    [[nodiscard]]ChordNode getNode(const std::string& address) const;//!!!<DONE>implement this on the server side as well
    void pred_RPC(const std::string& remote_address, const std::string& new_address) const;//!!!<DONE>implement this on the server side as well
    void finger_RPC(const Identifier &s_id, const std::string &s_address, int i) const;//!!!implement this on the server side as well
public:
    ChordNode();
    explicit ChordNode(const std::string &n_prime_address);
    ChordNode(const ChordNode& other);

    [[nodiscard]]std::vector<char> serialize();

    void printId() const;
    void printAddress() const;

    [[nodiscard]]std::string getAddress() const;

    void setPred_address(const std::string& new_pred_address);
    void setFinger(int index, const Identifier &new_node_id, const std::string &new_node_address);

    [[nodiscard]]std::string closest_preceding_finger(const Identifier &id) const;
    [[nodiscard]]ChordNode find_predecessor(const Identifier &id) const;
    [[nodiscard]]ChordNode find_successor(const Identifier &id) const;

    void update_finger_table(const Identifier& s_id, const std::string &s_address, int i, const std::string &original_address);
    void update_others() const;
    void init_finger_table(const std::string &n_prime_address);
    void join(const std::string &address);

    bool operator==(const Identifier &other) const;
    bool operator!=(const Identifier &other) const;
    bool operator<(const Identifier &other) const;
    bool operator<=(const Identifier &other) const;
    bool operator>(const Identifier &other) const;
    bool operator>=(const Identifier &other) const;
    
    bool operator==(const ChordNode &other) const;
    bool operator!=(const ChordNode &other) const;
    bool operator<(const ChordNode &other) const;
    bool operator<=(const ChordNode &other) const;
    bool operator>(const ChordNode &other) const;
    bool operator>=(const ChordNode &other) const;

    ChordNode& operator=(const ChordNode &other);

    void printBytes() const;
    void printNodeInfo() const;
    void printPred() const;
    void printSucc() const;
    void printNodeId() const;
};

struct Finger {
    Identifier start, node_id;
    std::string node_address;
};

#endif //CHORDNODE_H
