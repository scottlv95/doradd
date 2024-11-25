#include <cassert>
#include <functional>
#include <ostream>
#include <vector>

//https://github.com/ibogosavljevic/johnysswlab/blob/master/2020-11-cachebatchprocessing/fast_hash_map.h

template <typename T>
class simple_hash_map_entry {
   public:
    bool find(const T& value) {
        return std::find(m_values.begin(), m_values.end(), value) !=
               m_values.end();
    }

    bool insert(const T& value) {
        if (find(value)) {
            return false;
        }

        m_values.push_back(value);
        return true;
    }

    void prefetch() { __builtin_prefetch(&m_values); }

    bool remove(const T& value) {
        auto it = std::find(m_values.begin(), m_values.end(), value);
        if (it != m_values.end()) {
            std::iter_swap(it, m_values.rbegin());
            m_values.pop_back();
            return true;
        } else {
            return false;
        }
    }

   private:
    std::vector<T> m_values;
};

template <typename T, typename Q>
class fast_hash_map {
   public:
    fast_hash_map(size_t size) : m_values(size), m_size(size) {}

    ~fast_hash_map() {}

    bool insert(const T& value) {
        size_t entry = get_entry(value);

        return m_values[entry].insert(value);
    }

    bool remove(const T& value) {
        size_t entry = get_entry(value);

        return m_values[entry].remove(value);
    }

    bool find(const T& value) {
        size_t entry = get_entry(value);
        return m_values[entry].find(value);
    }

    std::vector<bool> find_multiple_simple(const std::vector<T>& values) {
        std::vector<bool> result(values.size(), false);

        for (size_t i = 0; i < values.size(); i++) {
            result[i] = find(values[i]);
        }

        return result;
    }

    void dump(std::ostream& os) {
        for (size_t i = 0; i < m_values.size(); i++) {
            os << i << ": ";
            m_values[i].dump(os);
            os << std::endl;
        }
    }

    template <size_t look_ahead = 16>
    std::vector<bool> find_multiple_interleave(const std::vector<T>& values) {
        std::vector<bool> result(values.size(), false);
        std::array<size_t, look_ahead> hashes;

        for (size_t i = 0; i < hashes.size(); i++) {
            hashes[i] = get_entry(values[i]);
        }
 
        // Interleaving - Search from 0, at meanwhile, prefetch from look_ahead
        for (size_t i = 0, j = look_ahead; i < values.size() - look_ahead;
             i++, j++) {
            // access using prefetched items
            size_t entry = hashes[i % hashes.size()];
            result[i] = m_values[entry].find(values[i]);

            // pre-compute the hashes for values from idx-look_ahead
            // update the hash buffer and prefetch
            entry = get_entry(values[j]);
            hashes[j % hashes.size()] = entry;
            m_values[entry].prefetch();
        }

        // complete all find operations
        for (size_t i = values.size() - look_ahead; i < values.size(); i++) {
            result[i] = find(values[i]);
        }

        return result;
    }

    template <size_t look_ahead = 64>
    std::vector<bool> find_multiple_batch(const std::vector<T>& values) {
        std::vector<bool> result(values.size(), false);
        // bounded-size lookahead buffer to store keys/hashes
        std::array<size_t, look_ahead> hashes;
        size_t entry, index;

        size_t len = (values.size() / look_ahead) * look_ahead;

        for (size_t i = 0; i < len; i += look_ahead) {
            // batch prefetching
            for (size_t j = i, k = 0; k < look_ahead; j++, k++) {
                entry = get_entry(values[j]);
                hashes[k] = entry;
                m_values[entry].prefetch();
            }

            // then searching
            for (size_t j = i, k = 0; k < look_ahead; j++, k++) {
                result[j] = m_values[hashes[k]].find(values[j]);
            }
        }

        for (size_t i = len; i < values.size(); i++) {
            result[i] = find(values[i]);
        }

        return result;
    }

    std::vector<Q> m_values;
    size_t m_size;
    std::hash<T> m_hash;

    size_t get_entry(const T& v) { return m_hash(v) % m_size; }
};
