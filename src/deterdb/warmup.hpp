#pragma once

#include <iostream>


class Body
{
public:
  ~Body()
  {
    std::cout << "Body destroyed" << std::endl;
  }
};

static void prepare_run()
{
  // warm up workers
  auto log = make_cown<Body>();
  for(int i = 0; i < 10; i++)
    when(log) << [](auto) {/*std::cout << "log" << std::endl;*/};
    
}
