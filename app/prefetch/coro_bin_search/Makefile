CXX=g++
FLAGS=-O2 -fcoroutines -std=c++20 #-stdlib=libc++

a.out:	nanotest.cpp Makefile rng.h naive.h sm.h coro.h
	$(CXX) $(FLAGS) nanotest.cpp -o nanotest
