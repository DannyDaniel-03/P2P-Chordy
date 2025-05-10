#ifndef IDENTIFIER_H
#define IDENTIFIER_H

#include <string>
#include <bitset>
#include <vector>

class Identifier {
    uint8_t *bytes;//big endian order
public:
    static int length();
    static int size();
//stmh
    Identifier();
    explicit Identifier(int p);//used to initialize 2^p
    explicit Identifier(const char* input);
    explicit Identifier(const std::string &input);
    Identifier(const Identifier &other);
    ~Identifier();

    bool operator==(const Identifier &other) const;
    bool operator!=(const Identifier &other) const;
    bool operator<(const Identifier &other) const;
    bool operator<=(const Identifier &other) const;
    bool operator>(const Identifier &other) const;
    bool operator>=(const Identifier &other) const;

    void set(const std::vector<char> &input, long offset);

    Identifier operator+(const Identifier &other) const;
    Identifier operator-(const Identifier &other) const;
    Identifier& operator=(const Identifier &other);
    Identifier& operator=(Identifier &&other) noexcept;

    [[nodiscard]]uint8_t* begin();
    [[nodiscard]]const uint8_t* begin() const;
    [[nodiscard]]uint8_t* end();
    [[nodiscard]]const uint8_t* end() const;

    void print() const;
    void printBytes() const;
};

#endif //IDENTIFIER_H
