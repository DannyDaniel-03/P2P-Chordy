cmake_minimum_required(VERSION 3.29)
project(Proiect)

set(CMAKE_CXX_STANDARD 20)

# Find OpenSSL
find_package(OpenSSL REQUIRED)

# OpenSSLExample executable
add_executable(OpenSSLExample Identifier.h
        Identifier.cpp)
target_link_libraries(OpenSSLExample PRIVATE OpenSSL::SSL OpenSSL::Crypto)

add_executable(Proiect main.cpp
        Identifier.cpp
        Identifier.h
        ChordNode.cpp
        ChordNode.h
)
target_link_libraries(Proiect PRIVATE OpenSSL::SSL OpenSSL::Crypto)