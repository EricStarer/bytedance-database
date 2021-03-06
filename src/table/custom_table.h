#pragma once
#include "table.h"
#include <vector>
#include <map>

#ifndef FIXED_BYTE_LEN
#define FIXED_BYTE_LEN (2)
#endif

namespace bytedance_db_project {
//
// Custom table implementation to adapt to provided query mix.
//
class CustomTable : Table {
public:
  CustomTable();
  ~CustomTable();

  // Loads data into the table through passed-in data loader. Is not timed.
  void Load(BaseDataLoader *loader) override;

  // Returns the int32_t field at row `row_id` and column `col_id`.
  int32_t GetIntField(int32_t row_id, int32_t col_id) override;

  // Inserts the passed-in int32_t field at row `row_id` and column `col_id`.
  void PutIntField(int32_t row_id, int32_t col_id, int32_t field) override;
  void PutGetPredicatedUpdate(int32_t row_id);
  // Implements the query
  // SELECT SUM(col0) FROM table;
  // Returns the sum of all elements in the first column of the table.
  int64_t ColumnSum() override;

  // Implements the query
  // SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
  // Returns the sum of all elements in the first column of the table,
  // subject to the passed-in predicates.
  int64_t PredicatedColumnSum(int32_t threshold1, int32_t threshold2) override;

  // Implements the query
  // SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 >
  // threshold; Returns the sum of all elements in the rows which pass the
  // predicate.
  int64_t PredicatedAllColumnsSum(int32_t threshold) override;

  // Implements the query
  // UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
  // Returns the number of rows updated.
  int64_t PredicatedUpdate(int32_t threshold) override;

private:
  int64_t sum_col0_{0};
  std::vector<int32_t> row_sum_array_;
  int32_t num_cols_{0};
  int32_t num_rows_{0};
  char *row_first4_{nullptr};
  char *column_last_{nullptr};
  std::map<int16_t, std::vector<int32_t>> index_col0_; 
  std::map<int16_t, std::vector<int32_t>> index_col1_;
};
} // namespace bytedance_db_project
