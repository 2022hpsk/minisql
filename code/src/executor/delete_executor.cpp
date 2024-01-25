//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  //same with insert!
  //init child_executor_
  child_executor_->Init();
  // init table_info_
  table_info_ = TableInfo::Create();
  exec_ctx_->GetCatalog()->GetTable(plan_->table_name_,table_info_);
  // init index_info_vec_
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),index_info_vec_);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  RowId child_rid;
  Row child_row;
  if(!child_executor_->Next(&child_row,&child_rid))return false;
  if(!table_info_->GetTableHeap()->MarkDelete(child_row.GetRowId(), exec_ctx_->GetTransaction()))return false;
  for(auto it = index_info_vec_.begin();it!=index_info_vec_.end();it++){//遍历所有index，删除其中的child构成的key
    auto keySchema = (*it)->GetIndexKeySchema();
    vector<Field> fields;
    for( auto col : keySchema->GetColumns() ){
      uint32_t col_index;
      table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
      fields.push_back(*(child_row.GetField(col_index)));
    }
    Row key_to_insert(fields);
    (*it)->GetIndex()->RemoveEntry(key_to_insert,child_row.GetRowId(),exec_ctx_->GetTransaction());
  }
  return true;
}



