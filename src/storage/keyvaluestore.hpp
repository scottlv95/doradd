#ifndef IKEYVALUESTORE_H
#define IKEYVALUESTORE_H

#include <string>
#include <vector>
#include <utility>

class IKeyValueStore {
public:
    virtual ~IKeyValueStore() {}

    virtual bool open(const std::string& db_path) = 0;

    virtual bool put(const std::string& key, const std::string& value) = 0;

    virtual bool get(const std::string& key, std::string& value) = 0;

    virtual bool writeBatch(const std::vector<std::pair<std::string, std::string>>& entries) = 0;

    virtual void close() = 0;
};

#endif