//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
    table_info_ = TableInfo::Create();;
    exec_ctx_->GetCatalog()->GetTable(plan_->table_name_, table_info_);
    itr_ = table_info_->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
    while(itr_!=table_info_->GetTableHeap()->End() ){
      //如果是空谓词，直接全都返回；或者谓词判定后该row符合条件，也返回
      if(!plan_->filter_predicate_||Field(kTypeInt, 1).CompareEquals(plan_->filter_predicate_->Evaluate(&(*itr_)))){
        //只输出所需的field构成的row！！！！！
        //这里在seq的测试中测不出来，反应在了delete的测试中
        vector<Field> out_fields;
        out_fields.clear();
        uint32_t column_index;
        for( auto column_p : plan_->OutputSchema()->GetColumns() ){
          table_info_->GetSchema()->GetColumnIndex(column_p->GetName(),column_index);
          out_fields.push_back(*(*itr_).GetField(column_index));
        }
        *row = Row(out_fields);
        row->SetRowId((*itr_).GetRowId());
        //*rid = (itr_)->GetRowId(); fix bug when find bug in #2
        rid = new RowId(row->GetRowId());
        itr_++;
        return true;
      }
      itr_++;
    }
    //遍历结束
    return false;
}
