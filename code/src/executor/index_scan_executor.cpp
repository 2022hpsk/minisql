#include "executor/executors/index_scan_executor.h"
#include <map>
#include <vector>
#include <set>
#include <algorithm>
#include <iterator>
#include "planner/expressions/constant_value_expression.h"
bool ridcomp(const RowId &r1,const RowId &r2){
  if(r1.GetPageId()!=r2.GetPageId()){
    return r1.GetPageId()<r2.GetPageId();
  }else{
    return r1.GetSlotNum()<r2.GetSlotNum();
  }
}
/**
* TODO: Student Implement
 */
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void FindIndexColumn(const AbstractExpressionRef &filter_predicate_, map<uint32_t, pair<string, Field>> &map_){
  if(filter_predicate_->GetChildAt(0)->GetType() != ExpressionType::ColumnExpression ||
     filter_predicate_->GetChildAt(1)->GetType() != ExpressionType::ConstantExpression||
     filter_predicate_->GetType() != ExpressionType::ComparisonExpression ){
    FindIndexColumn(filter_predicate_->GetChildAt(0), map_);
    FindIndexColumn(filter_predicate_->GetChildAt(1), map_);
  }else{
    auto column_value_expr = dynamic_pointer_cast<ColumnValueExpression>(filter_predicate_->GetChildAt(0));
    auto constant_value_expr = dynamic_pointer_cast<ConstantValueExpression>(filter_predicate_->GetChildAt(1));
    auto comparison_expr = dynamic_pointer_cast<ComparisonExpression>(filter_predicate_);
    pair<string, Field> pair_(comparison_expr->GetComparisonType(), constant_value_expr->val_);
    map_.insert(map<uint32_t, pair<string, Field>>::value_type((*&column_value_expr)->GetColIdx(), pair_));
  }
}
void FindIndexColumn(const vector<IndexInfo *> &index_info_vec_, map<uint32_t, uint32_t> &map_2){
  int has_index[1000]={0};//最大column size为1000
  for(int i = 0; i < 1000; i++) has_index[i] = 0;
  for(uint32_t i = 0; i < index_info_vec_.size(); i++){
    uint32_t column_idx = (index_info_vec_[i]->GetIndexKeySchema()->GetColumns())[0]->GetTableInd();
    if(has_index[column_idx] == 0){
      has_index[column_idx] = 1;
      map_2.insert(map<uint32_t, uint32_t>::value_type(i, column_idx));
    }
  }
}

void IndexScanExecutor::Init() {
  map<uint32_t, pair<string, Field>> map0;
  map<uint32_t, uint32_t> map1;
  FindIndexColumn(plan_->GetPredicate(), map0);
  FindIndexColumn(plan_->indexes_, map1);
  //insert the first then go for cycle
  auto it = map1.begin();
  vector<Field> key_field;
  key_field.push_back(map0.at(it->second).second);
  (plan_->indexes_)[0]->GetIndex()->ScanKey(Row(key_field), rid_vec_, nullptr, (map0.at(it->second)).first);
  for(it++; it != map1.end(); it++){
    vector<RowId> res;
    vector<Field> key_field_;
    key_field_.push_back((map0.at(it->second)).second);
    (plan_->indexes_)[it->first]->GetIndex()->ScanKey(Row(key_field_), res, nullptr, (map0.at(it->second)).first);
    sort(res.begin(), res.end(), ridcomp);
    auto itr = set_intersection(res.begin(), res.end(),rid_vec_.begin(), rid_vec_.end(), rid_vec_.begin(), ridcomp);
    rid_vec_.resize(itr - rid_vec_.begin());
  }
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  if(count < rid_vec_.size()){
    *rid = rid_vec_[count];
    TableInfo *table_info_=TableInfo::Create();
    GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
    row->SetRowId(rid_vec_[count]);
    row->GetFields().clear();
    table_info_->GetTableHeap()->GetTuple(row, exec_ctx_->GetTransaction());
    count++;
    return true;
  }
  else
    return false;

}
