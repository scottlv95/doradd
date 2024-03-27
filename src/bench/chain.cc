#include "constants.hpp"
#include "db.hpp"
#include "chain/constants.hpp"
#include "pipeline.hpp"
#include "chain/db.hpp"
#include "chain/generator.hpp"
#include "chain/macros.hpp"
#include "chain/generic_macros.hpp"
#include "txcounter.hpp"

#include <thread>
#include <unordered_map>
#include <cpp/when.h>

using namespace verona::rt;
using namespace verona::cpp;

template<typename T> 
struct ChainTransaction
{
public:
  static Database<T>* index;
  
#if defined(INDEXER) || defined(TEST_TWO)
  static int prepare_cowns(char* input)
  {
    auto txm = reinterpret_cast<typename T::Marshalled*>(input);

    if constexpr (std::is_same_v<T, Mixed>) {
      for (int i = 0; i < txm->num_writes; i++) {
        txm->cown_ptrs[i] = index->resource_table.get_row_addr(txm->params[i] - 1)->get_base_addr();
      }
    } else {
      int i, j;
      for (i = 0; i < T::NUM_RESRC_COWN; i++)
        txm->cown_ptrs[i] = index->resource_table.get_row_addr(txm->params[i] - 1)->get_base_addr();
      for (j = i; j < T::NUM_COWN; j++)
        txm->cown_ptrs[j] = index->user_table.get_row_addr(txm->params[j] - 1)->get_base_addr();
    }

    return T::MarshalledSize;
  }
  
  // agnostic prefetching (not bound with variable Mixed::num_writes) 
  static int prefetch_cowns(const char* input)
  {
    auto txm = reinterpret_cast<const typename T::Marshalled*>(input);

    for (int i = 0; i < T::NUM_COWN; i++)
      __builtin_prefetch(reinterpret_cast<const void*>(txm->cown_ptrs[i]), 1, 3);

    return T::MarshalledSize; 
  } 
#endif

#ifdef RPC_LATENCY
  static int parse_and_process(const char* input, ts_type init_time)
#else
  static int parse_and_process(const char* input)
#endif // RPC_LATENCY
  {
    auto txm = reinterpret_cast<const typename T::Marshalled*>(input);
    if constexpr (std::is_same_v<T, Mixed>) {
      uint32_t gas = txm->gas;
      switch(txm->num_writes) {
      case 1:{
        EVAL(REPEAT(1, GET_COWN, ~))
        WHEN_C(r0)
        PARAMS(auto _r0)
        BODY(); break;
        }
      case 2:{
        EVAL(REPEAT(2, GET_COWN, ~))
        WHEN_C(r0, r1)
        PARAMS(auto _r0, auto _r1)
        BODY(); break;
        }
      case 3:{
        EVAL(REPEAT(3, GET_COWN, ~))
        WHEN_C(r0, r1, r2)
        PARAMS(auto _r0, auto _r1, auto _r2)
        BODY(); break;
        }
      case 4:{
        EVAL(REPEAT(4, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3)
        BODY(); break;
        }
      case 5:{
        EVAL(REPEAT(5, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4)
        BODY(); break;
        }
      case 6:{
        EVAL(REPEAT(6, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5)
        BODY(); break;
        }
      case 7:{
        EVAL(REPEAT(7, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6)
        BODY(); break;
        }
      case 8:{
        EVAL(REPEAT(8, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7)
        BODY(); break;
        }
      case 9: {
        EVAL(REPEAT(9, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8)
        BODY(); break;
      }
      case 10: {
        EVAL(REPEAT(10, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9)
        BODY(); break;
      }
      case 11: {
        EVAL(REPEAT(11, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10)
        BODY(); break;
      }
      case 12: {
        EVAL(REPEAT(12, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11)
        BODY(); break;
      }
      case 13: {
        EVAL(REPEAT(13, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12)
        BODY(); break;
      }
      case 14: {
        EVAL(REPEAT(14, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13)
        BODY(); break;
      }
      case 15: {
        EVAL(REPEAT(15, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14)
        BODY(); break;
      }
      case 16: {
        EVAL(REPEAT(16, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15)
        BODY(); break;
      }
      case 17: {
        EVAL(REPEAT(17, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16)
        BODY(); break;
      }
      case 18: {
        EVAL(REPEAT(18, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17)
        BODY(); break;
      }
      case 19: {
        EVAL(REPEAT(19, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18)
        BODY(); break;
      }
      case 20: {
        EVAL(REPEAT(20, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19)
        BODY(); break;
      }
      case 21: {
        EVAL(REPEAT(21, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20)
        BODY(); break;
      }
      case 22: {
        EVAL(REPEAT(22, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21)
        BODY(); break;
      }
      case 23: {
        EVAL(REPEAT(23, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22)
        BODY(); break;
      }
      case 24: {
        EVAL(REPEAT(24, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23)
        BODY(); break;
      }
      case 25: {
        EVAL(REPEAT(25, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24)
        BODY(); break;
      }
      case 26: {
        EVAL(REPEAT(26, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25)
        BODY(); break;
      }
      case 27: {
        EVAL(REPEAT(27, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26)
        BODY(); break;
      }
      case 28: {
        EVAL(REPEAT(28, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27)
        BODY(); break;
      }
      case 29: {
        EVAL(REPEAT(29, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28)
        BODY(); break;
      }
      case 30: {
        EVAL(REPEAT(30, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29)
        BODY(); break;
      }
      case 31: {
        EVAL(REPEAT(31, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30)
        BODY(); break;
      }
      case 32: {
        EVAL(REPEAT(32, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30, r31)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30, auto _r31)
        BODY(); break;
      }
      case 33: {
        EVAL(REPEAT(33, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30, r31, r32)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30, auto _r31, auto _r32)
        BODY(); break;
      }
      case 34: {
        EVAL(REPEAT(34, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30, r31, r32, r33)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30, auto _r31, auto _r32, auto _r33)
        BODY(); break;
      }
      case 35: {
        EVAL(REPEAT(35, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30, r31, r32, r33, r34)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30, auto _r31, auto _r32, auto _r33, auto _r34)
        BODY(); break;
      }
      case 36: {
        EVAL(REPEAT(36, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30, r31, r32, r33, r34, r35)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30, auto _r31, auto _r32, auto _r33, auto _r34, auto _r35)
        BODY(); break;
      }
      case 37: {
        EVAL(REPEAT(37, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30, r31, r32, r33, r34, r35, r36)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30, auto _r31, auto _r32, auto _r33, auto _r34, auto _r35, auto _r36)
        BODY(); break;
      }
      case 38: {
        EVAL(REPEAT(38, GET_COWN, ~))
        WHEN_C(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30, r31, r32, r33, r34, r35, r36, r37)
        PARAMS(auto _r0, auto _r1, auto _r2, auto _r3, auto _r4, auto _r5, auto _r6, auto _r7, auto _r8, auto _r9, auto _r10, auto _r11, auto _r12, auto _r13, auto _r14, auto _r15, auto _r16, auto _r17, auto _r18, auto _r19, auto _r20, auto _r21, auto _r22, auto _r23, auto _r24, auto _r25, auto _r26, auto _r27, auto _r28, auto _r29, auto _r30, auto _r31, auto _r32, auto _r33, auto _r34, auto _r35, auto _r36, auto _r37)
        BODY(); break;
      }
      default:
        break;
      }
    } else if constexpr (std::is_same_v<T, Nft>) {      
      cown_ptr<Resource> r = get_cown_ptr_from_addr<Resource>(
        reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<User> u = get_cown_ptr_from_addr<User>(
        reinterpret_cast<void*>(txm->cown_ptrs[1]));

#ifdef RPC_LATENCY
      when(r, u) << [init_time]
#else
      when(r, u) << []
#endif
      (auto _r, auto _u)
      {
        SPIN_RUN();
        M_LOG_LATENCY();
      };
    } else if constexpr (std::is_same_v<T, P2p>) {
    
      cown_ptr<User> s = get_cown_ptr_from_addr<User>(
        reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<User> r = get_cown_ptr_from_addr<User>(
        reinterpret_cast<void*>(txm->cown_ptrs[1]));

#ifdef RPC_LATENCY
      when(s, r) << [init_time]
#else
      when(s, r) << []
#endif
      (auto _s, auto _r)
      {
        SPIN_RUN();
        M_LOG_LATENCY();
      };
    } else if constexpr (std::is_same_v<T, Dex>) {  
      cown_ptr<Resource> r = get_cown_ptr_from_addr<Resource>(
        reinterpret_cast<void*>(txm->cown_ptrs[0]));

#ifdef RPC_LATENCY
      when(r) << [init_time]
#else
      when(r) << []
#endif
      (auto _r)
      {
        SPIN_RUN();
        M_LOG_LATENCY();
      };
    }
    return T::MarshalledSize; 
  }

  ChainTransaction(const ChainTransaction&) = delete;
  ChainTransaction& operator=(const ChainTransaction&) = delete;
};

using TxnType = P2p;
template <> 
Database<TxnType>* ChainTransaction<TxnType>::index = nullptr;

int main(int argc, char** argv)
{
  /* input args parsing */
  if (argc != 8 || strcmp(argv[1], "-n") != 0 || strcmp(argv[3], "-l") != 0)
  {
    fprintf(stderr, "Usage: ./program -n core_cnt -l look_ahead"  
      " <dispatcher_input_file> -i <inter_arrival>\n");
    return -1;
  }
  uint8_t core_cnt = atoi(argv[2]);
  uint8_t max_core = std::thread::hardware_concurrency();
  assert(1 < core_cnt && core_cnt <= max_core);  
  size_t look_ahead = atoi(argv[4]);
  assert(8 <= look_ahead && look_ahead <= 128);
  char* input_file = argv[5];
  char* gen_file = argv[7];

  /* Init tables and resources */
  auto& db = ChainTransaction<TxnType>::index;
  db = new Database<TxnType>();
    
  // Create cowns with huge pages and via static allocation
  void* chain_arr_addr_resource = static_cast<void*>(
    aligned_alloc_hpage(1024 * (TxnType::NUM_RESRC + NUM_ACCOUNTS)));
  void* chain_arr_addr_user = static_cast<void*>(
    static_cast<char*>(chain_arr_addr_resource) + 1024 * TxnType::NUM_RESRC);

  ChainGenerator<TxnType> gen(db, chain_arr_addr_resource, chain_arr_addr_user);
  db->resource_table.start_addr = (void*)chain_arr_addr_resource;
  db->user_table.start_addr = (void*)chain_arr_addr_user;
  
  gen.generateResources();
  gen.generateUsers();

  build_pipelines<ChainTransaction<TxnType>>(core_cnt - 1, input_file, gen_file);

  // Cleanup
  delete ChainTransaction<TxnType>::index;
}
