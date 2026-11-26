# frozen_string_literal: true

module Berylx
  module EffectTree
    # 並列ノード 1 回の実行に閉じたスレッド実行器。共有 pool は持たない
    # (workers.pool)。limit が nil なら無制限 (他枝の完了を待たず Thread を
    # 開始)、整数なら同時に走る枝を limit 本以下に抑える。StandardError でない
    # 例外が発生したら停止印を立て、未着手の枝は開始せず、開始済みの枝を定義順に
    # join してから定義順で最初の例外を再送出する。
    class ParallelExecutor
      def initialize(branches, limit, &run_branch)
        @branches = branches
        @limit = limit
        @run_branch = run_branch
        @running = 0
        @stopped = false
        @results = Array.new(branches.size)
        @exceptions = Array.new(branches.size)
        @mutex = Mutex.new
        @cv = ConditionVariable.new
      end

      def run
        threads = spawn_branches
        threads.each(&:join)
        raised = first_exception
        raise raised if raised

        @results
      end

      private

      def spawn_branches
        threads = []
        @branches.each_with_index do |branch, index|
          break unless acquire_slot

          thread = start_thread(branch, index)
          break unless thread

          threads << thread
        end
        threads
      rescue Exception => e # rubocop:disable Lint/RescueException
        stop!
        threads.each(&:join)
        raise e
      end

      def start_thread(branch, index)
        Thread.new { execute_branch(branch, index) }
      rescue Exception => e # rubocop:disable Lint/RescueException
        @stopped = true
        finish_branch(index, nil, e, false)
        nil
      end

      def execute_branch(branch, index)
        completed = false
        result = nil
        exception = nil
        return unless enter_branch

        result = @run_branch.call(branch)
        completed = true
      rescue Exception => e # rubocop:disable Lint/RescueException
        @stopped = true
        exception = e
      ensure
        finish_branch(index, result, exception, completed)
      end

      def acquire_slot
        @mutex.synchronize do
          @cv.wait(@mutex) while @limit && @running >= @limit && !@stopped
          return false if @stopped

          @running += 1
          true
        end
      end

      def enter_branch
        @mutex.synchronize { !@stopped }
      end

      def stop!
        @mutex.synchronize do
          @stopped = true
          @cv.broadcast
        end
      end

      def finish_branch(index, result, exception, completed)
        @mutex.synchronize do
          if exception
            @stopped = true
            @exceptions[index] = exception
          elsif completed
            @results[index] = result
          end
          @running -= 1
          @cv.signal
        end
      end

      def first_exception
        @mutex.synchronize { @exceptions.find { !_1.nil? } }
      end
    end
    private_constant :ParallelExecutor
  end
end
