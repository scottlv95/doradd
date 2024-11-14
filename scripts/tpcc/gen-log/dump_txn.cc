#include <cstdint>
#include <iostream>
#include <string>
#include <stdint.h>

struct TPCCTransactionMarshalled
{
  uint8_t txn_type; // 0 for NewOrder, 1 for Payment
  uint64_t cown_ptrs[30];
  uint32_t params[65];
  uint8_t padding[11];
} __attribute__((packed));

static_assert(sizeof(TPCCTransactionMarshalled) == 512);

// Read from file byte by byte
void read_file(){
    std::string filename = "tpcc.txt";

    printf("sizeof TPCCTransactionMarshalled: %lu\n", sizeof(TPCCTransactionMarshalled));

    FILE *fp = fopen(filename.c_str(), "rb");
    uint64_t num_txns;
    fread(&num_txns, sizeof(uint32_t), 1, fp);
    std::cout << "Number of transactions: " << num_txns << std::endl;

    for (uint64_t i = 0; i < num_txns; i++)
    {
        TPCCTransactionMarshalled txn;
        fread(&txn, sizeof(TPCCTransactionMarshalled), 1, fp);
        std::cout << "Transaction " << i << " type: " << (int)txn.txn_type << std::endl;
        for (int j = 0; j < 65; j++)
        {
            std::cout << txn.params[j] << " ";
        }
        std::cout << std::endl;
    }
}

int main(){
    read_file();
    return 0;
}