#include "Identifier.h"

#include <cstring>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <openssl/sha.h>

int Identifier::length() {
    return SHA256_DIGEST_LENGTH * 8;
}
//smth
int Identifier::size() {
    return SHA256_DIGEST_LENGTH;
}

Identifier::Identifier() {
    bytes = new uint8_t[SHA256_DIGEST_LENGTH];
}

Identifier::Identifier(int p) {
    if (p < 0 or p > 255)
        throw std::invalid_argument("Identifier::Identifier(int p): p belongs to [0," + std::to_string(SHA256_DIGEST_LENGTH) + ").");

    bytes = new uint8_t[SHA256_DIGEST_LENGTH];
    memset(bytes, 0, SHA256_DIGEST_LENGTH);

    p = 255 - p;//big-endian representation
    bytes[p/8] |= 128 >> p%8;
}

Identifier::Identifier(const char *input) {
    bytes = new uint8_t[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(input), strlen(input), bytes);
}

Identifier::Identifier(const std::string &input) {
    bytes = new uint8_t[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(input.c_str()), input.length(), bytes);
}

Identifier::Identifier(const Identifier &other) {
    bytes = new uint8_t[SHA256_DIGEST_LENGTH];
    memcpy(bytes, other.bytes, SHA256_DIGEST_LENGTH);
}

Identifier::~Identifier() {
    delete[] bytes;
}

bool Identifier::operator==(const Identifier &other) const {
    return memcmp(bytes, other.bytes, SHA256_DIGEST_LENGTH) == 0;
}

bool Identifier::operator!=(const Identifier &other) const {
    return !(*this == other);
}

bool Identifier::operator<(const Identifier &other) const {
    return memcmp(bytes, other.bytes, SHA256_DIGEST_LENGTH) < 0;
}

bool Identifier::operator<=(const Identifier &other) const {
    return *this < other or *this==other;
}

bool Identifier::operator>(const Identifier &other) const {
    return !(*this <= other);
}

bool Identifier::operator>=(const Identifier &other) const {
    return !(*this < other);
}

void Identifier::set(const std::vector<char> &input, long offset) {
    memcpy(bytes, input.data() + offset, SHA256_DIGEST_LENGTH);
}

Identifier Identifier::operator+(const Identifier &other) const {
    Identifier sum;

    for (int i = SHA256_DIGEST_LENGTH - 1, carry = 0; i >= 0; i--) {
        sum.bytes[i] = bytes[i] + other.bytes[i] + carry;
        carry = static_cast<uint16_t>(bytes[i]) + other.bytes[i] + carry > 255 ? 1 : 0;
    }

    return sum;
}

Identifier Identifier::operator-(const Identifier &other) const {
    Identifier diff;

    for (int i = SHA256_DIGEST_LENGTH - 1, borrow = 0; i >= 0; i--) {
        auto temp = static_cast<int16_t>(static_cast<int16_t>(bytes[i]) - static_cast<int16_t>(other.bytes[i]) - borrow);

        if (temp < 0) {
            temp += 256;
            borrow = 1;
        } else
            borrow = 0;

        diff.bytes[i] = static_cast<uint8_t>(temp);
    }

    return diff;
}

Identifier& Identifier::operator=(const Identifier &other) {
    if (this != &other) {
        delete[] bytes;
        bytes = new uint8_t[SHA256_DIGEST_LENGTH];
        memcpy(bytes, other.bytes, SHA256_DIGEST_LENGTH);
    }

    return *this;
}


Identifier& Identifier::operator=(Identifier &&other) noexcept {
    if (this != &other) {
        delete[] bytes;
        bytes = other.bytes;
        other.bytes = nullptr;
    }

    return *this;
}



uint8_t* Identifier::begin() {
    return bytes;
}

const uint8_t * Identifier::begin() const {
    return bytes;
}

uint8_t * Identifier::end() {
    return bytes + SHA256_DIGEST_LENGTH;
}

const uint8_t * Identifier::end() const {
    return bytes + SHA256_DIGEST_LENGTH;
}

void Identifier::print() const {
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(bytes[i]) << ' ';

    std::cout << std::endl;
}

void Identifier::printBytes() const {
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        std::cout << static_cast<int>(bytes[i]) << ' ';
}
