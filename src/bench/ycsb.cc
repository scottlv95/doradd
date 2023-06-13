#include <deterdb.hpp>
#include <dispatcher.hpp>
#include <perf.hpp>

constexpr uint32_t ROWS_PER_TX = 10;
constexpr uint32_t ROW_SIZE = 1000;
constexpr uint32_t WRITE_SIZE = 100;
const uint64_t ROW_COUNT = 1000000;

struct YCSBRow
{
  char payload[ROW_SIZE];
};

struct __attribute__((packed)) YCSBTransactionMarshalled
{
  uint64_t indices[ROWS_PER_TX];
  uint16_t write_set;
  uint8_t  pad[46];
};
static_assert(sizeof(YCSBTransactionMarshalled) == 128);

struct YCSBTransaction
{
public:
  Row<YCSBRow>* rows[ROWS_PER_TX];
  // Bit field. Assume less than 16 concurrent rows per transaction
  uint16_t write_set;
  uint16_t dispatcher_set;
  uint32_t row_count;
  static Index<YCSBRow>* index;
  static TxTerminator * tx_terminator;

  static int parse(const char* input, YCSBTransaction& tx)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    tx.write_set = txm->write_set;
    tx.row_count = ROWS_PER_TX;
    tx.dispatcher_set = 0;

    for (int i = 0; i < ROWS_PER_TX; i++) 
    {
      tx.rows[i] = index->get_row(txm->indices[i]);
#if 0
      auto disp_idx = ((((unsigned long)tx.rows[i]) & 0xff000) >> 12) % DISPATCHER_2ND_COUNT;
      tx.dispatcher_set |= (1 << disp_idx);
#endif
    }

    return sizeof(YCSBTransactionMarshalled);
  }

  void process() const
  {
    // Implement the transaction logic
    uint8_t sum = 0;
    uint16_t write_set_l = write_set;
    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      if (write_set_l & 0x1)
      {
        memset(&rows[i]->val, sum, WRITE_SIZE);
      }
      else
      {
        for (int j = 0; j < ROW_SIZE; j++)
          sum += rows[i]->val.payload[j];
      }
      write_set_l >>= 1;
    }
    
    // Count the transaction
    //schedule_lambda(tx_terminator, [](){ tx_terminator->count_tx(); });
  }
};

Index<YCSBRow>* YCSBTransaction::index;
TxTerminator* YCSBTransaction::tx_terminator;

int main(int argc, char** argv)
{
  auto& sched = Scheduler::get();
  //Scheduler::set_detect_leaks(true);
  //sched.set_fair(true);
  sched.init(8);

  if (argc != 2)
  {
    fprintf(stderr, "Usage ./ycsb <dispather_input_file>\n");
    return -1;
  }

  schedule_lambda([]() { std::cout << "Hello deterministic world!\n"; });

  // Create rows and populate index
  YCSBTransaction::index = new Index<YCSBRow>;
  auto& alloc = ThreadAlloc::get();
  for (int i = 0; i < ROW_COUNT; i++)
  {
    auto* r = new (alloc) Row<YCSBRow>;
    Cown::acquire(r);
    YCSBTransaction::index->insert_row(r);
  }

  // Create tx terminator for accounting
  YCSBTransaction::tx_terminator = new (alloc) TxTerminator;

  auto* dispatcher = new (alloc) FileDispatcher<YCSBTransaction>(argv[1], 1000);
  schedule_lambda(dispatcher, [=]() { dispatcher->run(); });

  sched.run();
}
