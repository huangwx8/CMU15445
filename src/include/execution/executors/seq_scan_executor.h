//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), 
    plan_(plan), 
    iter_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())), 
    end_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {}

  void Init() override {}

  bool Next(Tuple *tuple) override {
    for (; iter_ != end_; ++iter_) {
      Tuple it_tuple = *iter_;
      if (!plan_->GetPredicate() || plan_->GetPredicate()->Evaluate(&it_tuple, GetOutputSchema()).GetAs<bool>()) {
        *tuple = it_tuple;
        ++iter_;
        return true;
      }
    }
    return false;
  }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  /** The iterator which scans over the table */
  TableIterator iter_;
  /** A stop sign */
  TableIterator end_;
};
}  // namespace bustub
