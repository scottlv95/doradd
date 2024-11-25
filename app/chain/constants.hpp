#pragma once

#include <stdint.h>

static constexpr uint64_t NUM_ACCOUNTS = 289023;

class Mixed {
public:
  static constexpr uint8_t NUM_COWN  = 38;
  static constexpr uint8_t NUM_RESRC_COWN = 38;
  static constexpr uint64_t NUM_RESRC  = 41640;
  static constexpr uint64_t MAX_LENGTH = 38;  // write len
  static constexpr uint64_t MAX_GAS    = 2270;  // run time
  static constexpr uint16_t MarshalledSize = 512;
  
  struct __attribute__((packed)) Marshalled
  {
    uint8_t  num_writes;
    uint32_t gas;   
    uint32_t params[Mixed::NUM_COWN];
    uint64_t cown_ptrs[Mixed::NUM_COWN];
    uint8_t  pad[51];
  };
  static_assert(sizeof(Mixed::Marshalled) == Mixed::MarshalledSize);
};

class Nft {
public:
  static constexpr uint8_t  NUM_COWN   = 2;
  static constexpr uint8_t  NUM_RESRC_COWN = 1;
  static constexpr uint64_t NUM_SENDER = 10783;
  static constexpr uint64_t NUM_RESRC  = 844;
  static constexpr uint8_t  MarshalledSize = 64;
  static constexpr long SERV_TIME = 240'000;
  
  struct __attribute__((packed)) Marshalled
  {
    uint32_t params[Nft::NUM_COWN]; // resource and user
    uint64_t cown_ptrs[Nft::NUM_COWN];
    uint8_t  pad[40];
  };
  static_assert(sizeof(Nft::Marshalled) == Nft::MarshalledSize);
};

class P2p {
public:
  static constexpr uint8_t  NUM_COWN   = 2;
  static constexpr uint8_t  NUM_RESRC_COWN = 0;
  static constexpr uint64_t NUM_RESRC  = 0;
  static constexpr uint64_t NUM_SENDER = 51317;
  static constexpr uint64_t NUM_RECVER = 43456;
  static constexpr uint8_t  MarshalledSize = 64;
  static constexpr long SERV_TIME = 334'000;
  
  struct __attribute__((packed)) Marshalled
  {
    uint32_t params[P2p::NUM_COWN]; // sender and receiver
    uint64_t cown_ptrs[P2p::NUM_COWN];
    uint8_t  pad[40];
  };
  static_assert(sizeof(P2p::Marshalled) == P2p::MarshalledSize);
};

class Dex { // Uniswap
public:
  static constexpr uint8_t  NUM_COWN  = 1;
  static constexpr uint8_t  NUM_RESRC_COWN = 1;
  static constexpr uint8_t  MarshalledSize = 64;
  static constexpr uint64_t  NUM_RESRC = 330; // NUM_BUR
  static constexpr long SERV_TIME = 240'000;
 
  struct __attribute__((packed)) Marshalled
  {
    uint32_t params[Dex::NUM_COWN]; // sender and receiver
    uint64_t cown_ptrs[Dex::NUM_COWN];
    uint8_t  pad[52];
  };
  static_assert(sizeof(Dex::Marshalled) == Dex::MarshalledSize);

  /* static constexpr uint64_t NUM_AVG   = 267; */
  /* static constexpr uint64_t NUM_BUR   = 330; */
};
