#include "indexed_row_table.h"
#include <cstring>
#include <vector>
using std::vector;

namespace bytedance_db_project {
IndexedRowTable::IndexedRowTable(int32_t index_column) {
  index_column_ = index_column;
}

void IndexedRowTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
    int32_t col_val = *(int32_t *) (rows_ + FIXED_FIELD_LEN * (row_id * num_cols_ + index_column_));
    if(index_.count(col_val) == 0){
      index_[col_val] = vector<int32_t>{row_id};
    }
    else if(index_.count(col_val) == 1){
      index_[col_val].push_back(row_id);
    }
  }
}

int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int32_t res;
  res = *((int32_t *) (rows_ + FIXED_FIELD_LEN * (row_id * num_cols_ + col_id)));
  return res;
}

void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                  int32_t field) {
  // TODO: Implement this!
  int32_t raw_val = GetIntField(row_id, col_id);
  *((int32_t *) (rows_ + FIXED_FIELD_LEN * (row_id * num_cols_ + col_id))) = field;
  if(col_id == index_column_ && field != raw_val){
    for(size_t i = 0; i < index_[raw_val].size(); ++i){
      if(index_[raw_val][i] == row_id){
        index_[raw_val].erase(index_[raw_val].begin()+i);
        break;
      }
    }
    index_[field].push_back(row_id);
  }
}

int64_t IndexedRowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  for(auto row_id = 0; row_id < num_rows_; ++row_id){
    res += GetIntField(row_id, 0);
  }
  return res;
}

int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                             int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum = 0;
  if(index_column_ == 1){
    for(int32_t col_val = threshold1+1; col_val <= 1023; ++col_val){
      for(int32_t row_id : index_[col_val]){
        if(GetIntField(row_id, 2) < threshold2){
          sum += GetIntField(row_id, 0);
        }
      }
    }
  }
  else if(index_column_ == 2){
    for(int32_t col_val = 0; col_val < threshold2; ++col_val){
      for(int32_t row_id : index_[col_val]){
        if(GetIntField(row_id, 1) > threshold1){
          sum += GetIntField(row_id, 0);
        }
      }
    }
  }
  else{
    for(auto row_id = 0; row_id < num_rows_; ++row_id){
      if(GetIntField(row_id, 1) > threshold1 && GetIntField(row_id, 2) < threshold2){
        sum += GetIntField(row_id, 0);
      }
    }
  }
  return sum;
}

int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum = 0;
  if(index_column_ == 0){
    for(int32_t col_val = threshold+1; col_val <= 1023; ++col_val){
      for(int32_t row_id : index_[col_val]){
        for(int32_t col_id = 0; col_id < num_cols_; ++col_id){
          sum += GetIntField(row_id, col_id);
        }
      }
    }
  }
  else{
    for(auto row_id = 0; row_id < num_rows_; ++row_id){
      if(GetIntField(row_id, 0) > threshold){
        for(int32_t col_id = 0; col_id < num_cols_; ++col_id){
          sum += GetIntField(row_id, col_id);
        }
      }
    }
  }
  return sum;
}

int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t cnt = 0;
  if(index_column_ == 0){
    for(int32_t col_val = 0; col_val < threshold; ++col_val){
      for(int32_t row_id : index_[col_val]){
        PutIntField(row_id, 3, GetIntField(row_id, 2) + GetIntField(row_id, 3));
        cnt++;
      }
    }
  }
  else{
    for(auto row_id = 0; row_id < num_rows_; ++row_id){
      if(GetIntField(row_id, 0) < threshold){
        PutIntField(row_id, 3, GetIntField(row_id, 2) + GetIntField(row_id, 3));
        cnt++;
      }
    }
  }
  return cnt;
}
} // namespace bytedance_db_project