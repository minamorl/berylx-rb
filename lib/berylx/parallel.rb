# frozen_string_literal: true

module Berylx
  class Parallel
    UNSET = Object.new.freeze
    private_constant :UNSET

    attr_reader :branches, :reducer, :on_err

    def initialize(branches, reducer = Merge.deep, on_err: :short_circuit, workers: nil)
      @workers = workers.nil? ? nil : validate_workers!(workers)
      @branches = branches.flat_map { flatten_branch(_1) }.freeze
      @reducer = reducer
      @on_err = on_err
    end

    def workers(limit = UNSET)
      return @workers if limit.equal?(UNSET)

      self.class.new(@branches, @reducer, on_err: @on_err, workers: validate_workers!(limit))
    end

    def &(other)
      return self.class.new([self, other]) if @workers

      self.class.new(@branches + [other], @reducer, on_err: @on_err)
    end

    def >>(other)
      Sequence.new([self, other])
    end

    def reduce(reducer)
      self.class.new(@branches, reducer, on_err: @on_err, workers: @workers)
    end

    def short_circuit
      self.class.new(@branches, @reducer, on_err: :short_circuit, workers: @workers)
    end

    def accumulate
      self.class.new(@branches, @reducer, on_err: :accumulate, workers: @workers)
    end

    def rescue_with(handler = nil, name = nil, &)
      Sequence.build_rescue(self, handler, name, &)
    end

    # 実行は EffectTree に一本化。短絡 / accumulate / merge / workers の結果封筒
    # algebra と枝の開始上限は EffectTree.run_parallel に集約している (native の
    # 二重実装は撤去)。
    def call(focus)
      EffectTree.run(self, focus)
    end

    def compile
      Graph.from(self)
    end

    def nodes
      @branches.flat_map(&:nodes)
    end

    private

    def flatten_branch(branch)
      branch.is_a?(Parallel) && branch.workers.nil? ? branch.branches : [branch]
    end

    def validate_workers!(limit)
      return limit if limit.is_a?(Integer) && limit >= 1

      raise ArgumentError, "workers must be an Integer >= 1, got #{limit.inspect}"
    end
  end
end
