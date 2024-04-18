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

#include "catalog/simple_catalog.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_iterator.h"
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
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
      : AbstractExecutor(exec_ctx),
        plan_(plan),
        table_matedate_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
        table_iterator_(table_matedate_->table_->Begin(exec_ctx->GetTransaction())) {}

  void Init() override {}

  bool Next(Tuple *tuple) override {
    const auto *predicate = plan_->GetPredicate();
    while (table_iterator_ != table_matedate_->table_->End()) {
      *tuple = *table_iterator_;
      ++table_iterator_;
      if (predicate == nullptr || predicate->Evaluate(tuple, &table_matedate_->schema_).GetAs<bool>()) {
        return true;
      }
    }
    return false;
  }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  TableMetadata *table_matedate_;
  TableIterator table_iterator_;
};
}  // namespace bustub
