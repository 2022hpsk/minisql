//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  //init child_executor_
  child_executor_->Init();
  // init table_info_
  table_info_ = TableInfo::Create();
  exec_ctx_->GetCatalog()->GetTable(plan_->table_name_,table_info_);
  // init index_info_vec_
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),index_info_vec_);
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row childRow;
  RowId childRowId;
  if(!child_executor_->Next(&childRow,&childRowId))return false;
    Row newrow = GenerateUpdatedTuple(childRow);

    table_info_->GetTableHeap()->UpdateTuple(newrow,childRow.GetRowId(), exec_ctx_->GetTransaction());
    //更新所有index，删除key后插入key
    for(auto itr = index_info_vec_.begin();itr!=index_info_vec_.end();itr++){
      auto keySchema = (*itr)->GetIndexKeySchema();
      vector<Field>oldFields,newFields;
      for( auto col : keySchema->GetColumns() ){
        uint32_t col_index;
        table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
        oldFields.push_back(*(childRow.GetField(col_index)));
        newFields.push_back(*(newrow.GetField(col_index)));
      }
      Row oldkey(oldFields),newkey(newFields);
      (*itr)->GetIndex()->RemoveEntry(oldkey,childRowId, exec_ctx_->GetTransaction());
      (*itr)->GetIndex()->InsertEntry(newkey,newrow.GetRowId(), exec_ctx_->GetTransaction());
    }
    return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  Row newrow(src_row);

  for(auto itr = plan_->update_attrs_.begin();itr!=plan_->update_attrs_.end();itr++){
    Field newField = (*itr).second->Evaluate(&src_row);
    newrow.GetField((*itr).first)->operator=(newField);
  }
  return newrow;
}


