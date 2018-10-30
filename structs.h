#ifndef STRUCTS_H
#define STRUCTS_H

struct tuple {
  int32_t key;
  int32_t payload;
} tuple;

struct relation{
  struct tuple *tuples;
  int32_t num_tuples;
} relation;

#endif //STRUCTS