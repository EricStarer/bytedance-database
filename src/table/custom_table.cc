#include "custom_table.h"
#include <cstring>

namespace bytedance_db_project {
CustomTable::CustomTable() {}

CustomTable::~CustomTable() {}

void CustomTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  int32_t val;
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  row_sum_array = std::vector<int32_t>(num_rows_, 0);
  column_first4_ = new char[FIXED_BYTE_LEN * num_rows_ * 4];
  column_last_ = new char[FIXED_BYTE_LEN * num_rows_ * (num_cols_ - 4)];
  for(int32_t row_id = 0; row_id < num_rows_; row_id++){
    auto cur_row = rows.at(row_id);
    for(int32_t col_id = 0; col_id < num_cols_; col_id++){
      if(col_id < 4){
        int64_t offset = FIXED_BYTE_LEN * ((col_id*num_rows_)+row_id);
        val = *(int32_t*) (cur_row + FIXED_FIELD_LEN * col_id);
        *(int16_t*)(column_first4_+offset) = (int16_t)val;
      }
      else{
        int64_t offset = FIXED_BYTE_LEN * (((col_id-4)*num_rows_)+row_id);
        val = *(int32_t*) (cur_row + FIXED_FIELD_LEN * col_id);
        *(int16_t*)(column_last_+offset) = (int16_t)val;
      }
      if(col_id == 0){
        sum_col0_ += val;
      }
      row_sum_array[row_id] += val;
    }
  }
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int16_t val;
  if(col_id < 4){
    val = *(int16_t*)(column_first4_ + FIXED_BYTE_LEN*(col_id * num_rows_ + row_id));
  }
  else{
    val = *(int16_t*)(column_last_ + FIXED_BYTE_LEN*((col_id-4) * num_rows_ + row_id));
  }
  return (int32_t)val;
}

void CustomTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  int16_t raw_val = GetIntField(row_id, col_id);
  int16_t diff = field - raw_val;
  row_sum_array[row_id] += diff;
  if(col_id == 0){
    sum_col0_ += diff;
  }
  if(col_id < 4){
    *(int16_t*)(column_first4_ + FIXED_BYTE_LEN*(col_id * num_rows_ + row_id)) = (int16_t)field;
  }
  else{
    *(int16_t*)(column_last_ + FIXED_BYTE_LEN*((col_id-4) * num_rows_ + row_id)) = (int16_t)field;
  }
}

int64_t CustomTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = sum_col0_;
  return res;
}

int64_t CustomTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum = 0;
  for(auto row_id = 0; row_id < num_rows_; ++row_id){
    if(GetIntField(row_id, 1) > threshold1 && GetIntField(row_id, 2) < threshold2){
      sum += GetIntField(row_id, 0);
    }
  }
  return sum;
}

int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum = 0;
  for(auto row_id = 0; row_id < num_rows_; ++row_id){
    if(GetIntField(row_id, 0) > threshold){
      sum += row_sum_array[row_id];
    }
  }
  return sum;
}

int64_t CustomTable::PredicatedUpdate(int32_t threshold) {
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