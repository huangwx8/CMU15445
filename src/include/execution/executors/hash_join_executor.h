//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "container/hash/linear_probe_hash_table.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * IdentityHashFunction hashes everything to itself, i.e. h(x) = x.
 */
class IdentityHashFunction : public HashFunction<hash_t> {
 public:
  /**
   * Hashes the key.
   * @param key the key to be hashed
   * @return the hashed value
   */
  uint64_t GetHash(size_t key) override { return key; }
};

/**
 * A simple hash table that supports hash joins.
 */
class SimpleHashJoinHashTable {
 public:
  /** Creates a new simple hash join hash table. */
  SimpleHashJoinHashTable(const std::string &name, BufferPoolManager *bpm, HashComparator cmp, uint32_t buckets,
                          const IdentityHashFunction &hash_fn) {}

  /**
   * Inserts a (hash key, tuple) pair into the hash table.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param t the tuple to associate with the key
   * @return true if the insert succeeded
   */
  bool Insert(Transaction *txn, hash_t h, const Tuple &t) {
    hash_table_[h].emplace_back(t);
    return true;
  }

  /**
   * Gets the values in the hash table that match the given hash key.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param[out] t the list of tuples that matched the key
   */
  void GetValue(Transaction *txn, hash_t h, std::vector<Tuple> *t) { *t = hash_table_[h]; }

 private:
  std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
};

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
using HT = SimpleHashJoinHashTable;

// using HashJoinKeyType = ???;
// using HashJoinValType = ???;
// using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, HashComparator>;

/**
 * HashJoinExecutor executes hash join operations.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new hash join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the hash join plan node
   * @param left the left child, used by convention to build the hash table
   * @param right the right child, used by convention to probe the hash table
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan, std::unique_ptr<AbstractExecutor> &&left,
                   std::unique_ptr<AbstractExecutor> &&right)
      : AbstractExecutor(exec_ctx), 
        plan_(plan),
        left_(std::move(left)),
        right_(std::move(right)),
        jht_(
          std::string("SimpleHashJoinHashTable"), 
          exec_ctx_->GetBufferPoolManager(), 
          jht_comp_, 
          jht_num_buckets_, 
          jht_hash_fn_
        ) {}

  /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
  // Uncomment me! const HT *GetJHT() const { return &jht_; }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {}

  bool Next(Tuple *tuple) override {
    if (!inited_) {
      BuildLeftHashTable();
      inited_ = true;
    }

    const Schema* left_schema = plan_->GetLeftPlan()->OutputSchema();
    const Schema* right_schema = plan_->GetRightPlan()->OutputSchema();
    const Schema* out_schema = GetOutputSchema();

    while (left_visitor_index_ >= left_tuples_.size()) {
      if (!right_->Next(&right_tuple_)) {
        return false;
      } else {
        hash_t hash = HashValues(&right_tuple_, right_schema, plan_->GetRightKeys());
        jht_.GetValue(
          exec_ctx_->GetTransaction(), 
          hash, 
          &left_tuples_);
        left_visitor_index_ = 0;
      }
    }

    for (; left_visitor_index_ < left_tuples_.size(); left_visitor_index_++) {
      Tuple left_tuple = left_tuples_[left_visitor_index_];
      if (plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple_, right_schema).GetAs<bool>()) {
        std::vector<Value> out_values;
        for (const Column& col : out_schema->GetColumns()) {
          out_values.emplace_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple_, right_schema));
        }
        *tuple = Tuple(out_values, out_schema);
        left_visitor_index_++;
        return true;
      }
    }

    return Next(tuple);
  }

  /**
   * Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
   * @param tuple tuple to be hashed
   * @param schema schema to evaluate the tuple on
   * @param exprs expressions to evaluate the tuple with
   * @return the hashed tuple
   */
  hash_t HashValues(const Tuple *tuple, const Schema *schema, const std::vector<const AbstractExpression *> &exprs) {
    hash_t curr_hash = 0;
    // For every expression,
    for (const auto &expr : exprs) {
      // We evaluate the tuple on the expression and schema.
      Value val = expr->Evaluate(tuple, schema);
      // If this produces a value,
      if (!val.IsNull()) {
        // We combine the hash of that value into our current hash.
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }

 private:
  void BuildLeftHashTable() {
    const Schema* left_schema = plan_->GetLeftPlan()->OutputSchema();
    Tuple left_tuple;
    while (left_->Next(&left_tuple)) {
      hash_t hash = HashValues(&left_tuple, left_schema, plan_->GetLeftKeys());
      jht_.Insert(
        exec_ctx_->GetTransaction(), 
        hash, 
        left_tuple);
    }
  }

  /** The hash join plan node. */
  const HashJoinPlanNode *plan_;
  /** The comparator is used to compare hashes. */
  [[maybe_unused]] HashComparator jht_comp_{};
  /** The identity hash function. */
  IdentityHashFunction jht_hash_fn_{};

  std::unique_ptr<AbstractExecutor> left_;
  std::unique_ptr<AbstractExecutor> right_;

  /** The hash table that we are using. */
  HT jht_;
  /** The number of buckets in the hash table. */
  static constexpr uint32_t jht_num_buckets_ = 2;

  bool inited_ = false;

  std::vector<Tuple> left_tuples_;
  Tuple right_tuple_;
  size_t left_visitor_index_ = BUSTUB_INT32_MAX;
};
}  // namespace bustub
