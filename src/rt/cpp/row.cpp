#include <vector>
#include "cown_array.h"

std::vector<cown_ptr<RowType>> cows;
// Create an array of cown_ptrs first
std::vector<cown_ptr<RowType>> cow_ptrs;
for (uint64_t k : *keys_ptr) {
    if (auto p = index->get_row_addr(k))
        cow_ptrs.push_back(*p);
}
// Construct cown_array using the array of cown_ptrs
cown_array<RowType> cow_array(cow_ptrs.data(), cow_ptrs.size()); 