#include "row_table.h"
#include <cstring>

namespace bytedance_db_project {
RowTable::RowTable() {}

RowTable::~RowTable() {
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void RowTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
  }
}

int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int32_t res;
  res = *((int32_t *) (rows_ + FIXED_FIELD_LEN * (row_id * num_cols_ + col_id)));
  return res;
}

void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  *((int32_t *) (rows_ + FIXED_FIELD_LEN * (row_id * num_cols_ + col_id))) = field;
}

int64_t RowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  for(auto row_id = 0; row_id < num_rows_; ++row_id){
    res += GetIntField(row_id, 0);
  }
  return res;
}

int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum = 0;
  for(auto row_id = 0; row_id < num_rows_; ++row_id){
    if(GetIntField(row_id, 1) > threshold1 && GetIntField(row_id, 2) < threshold2){
      sum += GetIntField(row_id, 0);
    }
  }
  return sum;
}

int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum = 0;
  for(auto row_id = 0; row_id < num_rows_; ++row_id){
    if(GetIntField(row_id, 0) > threshold){
      for(int32_t col_id = 0; col_id < num_cols_; ++col_id){
        sum += GetIntField(row_id, col_id);
      }
    }
  }
  return sum;
}

int64_t RowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t cnt = 0;
  for(auto row_id = 0; row_id < num_rows_; ++row_id){
    if(GetIntField(row_id, 0) < threshold){
      PutIntField(row_id, 3, GetIntField(row_id, 2) + GetIntField(row_id, 3));
      cnt++;
    }
  }
  return cnt;
}
} // namespace bytedance_db_project