#pragma once

#include "constants.hpp"

#include <cpp/when.h>
#include <stdint.h>
#include <string>
#include <verona.h>

class __attribute__((packed)) Resource
{
public:
  uint64_t value;
  Resource() : value(0) {}
} __attribute__((aligned(256)));

class __attribute__((packed)) User
{
public:
  uint64_t value;
  User() : value(0) {}
} __attribute__((aligned(256)));
