# frozen_string_literal: true

require 'minitest/autorun'
require 'berylx'

# ==================================================================
# Parallel workers — spec-system pins/domains/berylx.spec v0.11
# (workers.*) の各 pin を 1 対 1 以上で検証するテスト群。
# すべての並列実行は Berylx::EffectTree を通る (第二の実行経路なし)。
# ==================================================================
class ParallelWorkersTest < Minitest::Test
  class CustomFatal < Exception; end # rubocop:disable Lint/InheritException

  # 決定的に同時実行を観測するためのタイムアウト付きバリア。
  class Barrier
    def initialize(parties, timeout: 5)
      @parties = parties
      @timeout = timeout
      @count = 0
      @mutex = Mutex.new
      @cv = ConditionVariable.new
    end

    def wait
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @timeout
      @mutex.synchronize do
        @count += 1
        if @count >= @parties
          @cv.broadcast
        else
          wait_for_parties(deadline)
        end
      end
    end

    private

    def wait_for_parties(deadline)
      while @count < @parties
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise 'barrier timed out' if remaining <= 0

        @cv.wait(@mutex, remaining)
      end
    end
  end

  # スレッドセーフに同時実行数と最大値を記録するカウンタ。
  class ConcurrencyTracker
    attr_reader :peak

    def initialize
      @active = 0
      @peak = 0
      @mutex = Mutex.new
    end

    def track
      @mutex.synchronize do
        @active += 1
        @peak = @active if @active > @peak
      end
      yield
    ensure
      @mutex.synchronize { @active -= 1 }
    end
  end

  def set_task(name, key, value)
    Berylx::Task[name] { |lay| lay[key].set(value) }
  end

  def drain(queue)
    items = []
    items << queue.pop until queue.empty?
    items
  end

  def build_tracked_branch(index, tracker, wave: nil, delay: 0)
    Berylx::Task[:"b#{index}"] do |lay|
      tracker.track do
        wave&.wait
        delay.positive? ? sleep(delay) : Thread.pass
        lay[:"k#{index}"].set(index)
      end
    end
  end

  def build_err_execution_branches(ran)
    [
      Berylx::Task[:reject_first] do |lay|
        ran << :reject_first
        lay.reject(:first_err, 'first')
      end,
      Berylx::Task[:raise_second] do |_lay|
        ran << :raise_second
        raise 'second'
      end,
      Berylx::Task[:ok_third] do |lay|
        ran << :ok_third
        lay[:third].set(true)
      end,
      Berylx::Task[:ok_fourth] do |lay|
        ran << :ok_fourth
        lay[:fourth].set(true)
      end
    ]
  end

  def build_await_all_branches(done)
    b1_started = Thread::Queue.new
    [
      Berylx::Task[:fast_fail] do |lay|
        b1_started.pop
        done[0] = true
        lay.reject(:fast_err, 'fast')
      end,
      Berylx::Task[:slow_ok] do |lay|
        b1_started << true
        sleep 0.03
        done[1] = true
        lay[:slow].set(true)
      end,
      Berylx::Task[:queued_ok] do |lay|
        sleep 0.02
        done[2] = true
        lay[:queued].set(true)
      end
    ]
  end

  def build_short_circuit_branches(ran)
    b1_done = Thread::Queue.new
    [
      Berylx::Task[:slow_first] do |lay|
        b1_done.pop
        ran << :slow_first
        lay[:from].set(:first).reject(:err_first, 'first')
      end,
      Berylx::Task[:fast_second] do |lay|
        ran << :fast_second
        b1_done << true
        lay[:from].set(:second).reject(:err_second, 'second')
      end,
      Berylx::Task[:queued_third] do |lay|
        ran << :queued_third
        lay[:from].set(:third).reject(:err_third, 'third')
      end
    ]
  end

  def build_join_before_raise_branches(fatal, release_b0, flags)
    b0_started = Thread::Queue.new
    [
      Berylx::Task[:b0] do |lay|
        b0_started << true
        release_b0.pop
        flags[:b0_finished] = true
        lay[:b0].set(true)
      end,
      Berylx::Task[:b1] do |_lay|
        b0_started.pop
        raise fatal
      end,
      Berylx::Task[:b2] do |lay|
        flags[:unstarted_ran] = true
        lay
      end
    ]
  end

  def build_two_raising_branches(err0, err1, flags)
    both_started = Barrier.new(2)
    b1_about_to_raise = Thread::Queue.new
    [
      Berylx::Task[:b0] do |_lay|
        both_started.wait
        b1_about_to_raise.pop
        sleep 0.02 # b1 が先に例外で終了するのを待つ
        raise err0
      end,
      Berylx::Task[:b1] do |_lay|
        both_started.wait
        b1_about_to_raise << true
        raise err1
      end,
      Berylx::Task[:b2] do |lay|
        flags[:unstarted_ran] = true
        lay
      end
    ]
  end

  def build_three_branch_exception_order(err1, err2, flags, join_order, join_mutex)
    all_started = Barrier.new(3)
    b2_about_to_raise = Thread::Queue.new
    [
      delayed_ok_task(:b0, all_started, flags) { record_thread_join(0, join_order, join_mutex) },
      delayed_raise_task(:b1, all_started, b2_about_to_raise, err1) { record_thread_join(1, join_order, join_mutex) },
      signaling_raise_task(:b2, all_started, b2_about_to_raise, err2) { record_thread_join(2, join_order, join_mutex) },
      flag_task(:b3, flags, :b3_ran)
    ]
  end

  def record_thread_join(index, join_order, join_mutex)
    Thread.current.define_singleton_method(:join) do |*args|
      join_mutex.synchronize { join_order << index }
      super(*args)
    end
  end

  def delayed_ok_task(name, barrier, flags)
    Berylx::Task[name] do |lay|
      yield
      barrier.wait
      sleep 0.03
      flags[:b0_finished] = true
      lay[name].set(true)
    end
  end

  def delayed_raise_task(name, barrier, signal, error)
    Berylx::Task[name] do |_lay|
      yield
      barrier.wait
      signal.pop
      sleep 0.015
      raise error
    end
  end

  def signaling_raise_task(name, barrier, signal, error)
    Berylx::Task[name] do |_lay|
      yield
      barrier.wait
      signal << true
      raise error
    end
  end

  def flag_task(name, flags, key)
    Berylx::Task[name] do |lay|
      flags[key] = true
      lay
    end
  end

  def build_nested_group(group, tracker, wave)
    tasks = (0...3).map do |index|
      Berylx::Task[:"g#{group}_t#{index}"] do |lay|
        tracker.track do
          wave.wait if group < 2 && index < 2
          Thread.pass
          lay[:"g#{group}_k#{index}"].set(index)
        end
      end
    end
    tasks.reduce(:&).workers(2)
  end

  # pin: workers.api
  def test_workers_api_public_name_and_immutability
    a = set_task(:a, :a, 1)
    b = set_task(:b, :b, 2)

    base = a & b
    limited = base.workers(2)
    relimited = limited.workers(3)
    via_new = Berylx::Parallel.new([a, b], workers: 2)
    configured = limited.reduce(Berylx::Merge.strict).accumulate.short_circuit

    assert_nil base.workers
    assert_equal 2, limited.workers
    assert_equal 3, relimited.workers
    assert_equal 2, via_new.workers
    assert_equal 2, configured.workers
  end

  # pin: workers.optional
  def test_workers_optional_defaults_to_omitted
    a = set_task(:a, :a, 1)
    b = set_task(:b, :b, 2)

    node = a & b
    from_new = Berylx::Parallel.new([a, b])

    assert_nil node.workers
    assert_nil from_new.workers
    assert_equal({ a: 1, b: 2 }, node.call(Berylx::Lay[]).focus.to_h)
  end

  # pin: workers.valid
  def test_workers_valid_accepts_integers_at_least_one
    a = set_task(:a, :a, 1)
    b = set_task(:b, :b, 2)

    [1, 2, 10].each do |limit|
      node = (a & b).workers(limit)
      constructed = Berylx::Parallel.new([a, b], workers: limit)

      assert_equal limit, node.workers
      assert_equal({ a: 1, b: 2 }, node.call(Berylx::Lay[]).focus.to_h)
      assert_equal limit, constructed.workers
      assert_equal({ a: 1, b: 2 }, constructed.call(Berylx::Lay[]).focus.to_h)
    end

    empty = Berylx::Parallel.new([], workers: 2)

    assert_equal({ base: 1 }, empty.call(Berylx::Lay[base: 1]).focus.to_h)
  end

  # pin: workers.invalid
  def test_workers_invalid_raises_argument_error_at_node_creation
    a = set_task(:a, :a, 1)
    b = set_task(:b, :b, 2)
    invalid_values = [0, -1, -100, 1.5, 2.0, '2', :two, true, false, Float::INFINITY, Float::NAN, Rational(2, 1)]

    invalid_values.each do |bad|
      assert_raises(ArgumentError) { (a & b).workers(bad) }
      assert_raises(ArgumentError) { Berylx::Parallel.new([a, b], workers: bad) }
    end

    assert_raises(ArgumentError) { (a & b).workers(nil) }
  end

  # pin: workers.limit
  def test_workers_limit_caps_simultaneously_running_branches
    tracker = ConcurrencyTracker.new
    first_wave = Barrier.new(2)
    branches = (0...8).map do |index|
      build_tracked_branch(index, tracker, wave: index < 2 ? first_wave : nil)
    end

    result = branches.reduce(:&).workers(2).call(Berylx::Lay[])

    assert_instance_of Berylx::Ok, result
    assert_equal 2, tracker.peak
    assert_equal((0...8).to_h { |i| [:"k#{i}", i] }, result.focus.to_h)
  end

  # pin: workers.limit (workers = 1 での厳密な直列上限)
  def test_workers_limit_one_runs_one_branch_at_a_time
    tracker = ConcurrencyTracker.new
    branches = (0...5).map { |index| build_tracked_branch(index, tracker, delay: 0.005) }

    result = branches.reduce(:&).workers(1).call(Berylx::Lay[])

    assert_instance_of Berylx::Ok, result
    assert_equal 1, tracker.peak
    assert_equal((0...5).to_h { |i| [:"k#{i}", i] }, result.focus.to_h)
  end

  # pin: workers.unbounded
  def test_workers_unbounded_starts_all_branches_without_waiting_for_completion
    barrier = Barrier.new(4)
    branches = (0...4).map do |index|
      Berylx::Task[:"u#{index}"] do |lay|
        barrier.wait
        lay[:"k#{index}"].set(index)
      end
    end

    result = branches.reduce(:&).call(Berylx::Lay[])

    assert_instance_of Berylx::Ok, result
    assert_equal({ k0: 0, k1: 1, k2: 2, k3: 3 }, result.focus.to_h)
  end

  # pin: workers.base
  def test_workers_base_every_branch_sees_same_start_lay
    seen = Array.new(4)
    branches = (0...4).map do |index|
      Berylx::Task[:"base#{index}"] do |lay|
        seen[index] = lay.to_h
        lay[:shared].set(index + 1)
      end
    end

    [1, 2].each do |limit|
      seen.fill(nil)
      result = branches.reduce(:&).workers(limit).call(Berylx::Lay[shared: 0, tag: :origin])

      assert_instance_of Berylx::Ok, result
      assert_equal Array.new(4, { shared: 0, tag: :origin }), seen
      assert_equal({ shared: 4, tag: :origin }, result.focus.to_h)
    end
  end

  # pin: workers.err_execution
  def test_workers_err_execution_runs_all_branches_in_both_modes
    %i[short_circuit accumulate].each do |mode|
      ran = Thread::Queue.new
      workflow = build_err_execution_branches(ran).reduce(:&).workers(1)
      workflow = mode == :accumulate ? workflow.accumulate : workflow.short_circuit

      result = workflow.call(Berylx::Lay[])

      assert_instance_of Berylx::Err, result
      assert_equal %i[reject_first raise_second ok_third ok_fourth], drain(ran)
    end
  end

  # pin: workers.await_all
  def test_workers_await_all_branches_before_returning_in_both_modes
    %i[short_circuit accumulate].each do |mode|
      done = Array.new(3, false)
      workflow = build_await_all_branches(done).reduce(:&).workers(2)
      workflow = mode == :accumulate ? workflow.accumulate : workflow.short_circuit

      result = workflow.call(Berylx::Lay[])

      assert_instance_of Berylx::Err, result
      assert_equal [true, true, true], done
    end
  end

  # pin: workers.fold_order
  def test_workers_fold_order_is_definition_order_not_completion_order
    branches = (1..4).map do |index|
      Berylx::Task[:"f#{index}"] do |lay|
        sleep((5 - index) * 0.01)
        lay[:id].set(index)
      end
    end
    order_collector = lambda do |left, right|
      Berylx::Lay[order: left.to_h.fetch(:order, []) + [right.to_h[:id]]]
    end

    bounded = branches.reduce(:&).workers(2).reduce(order_collector).call(Berylx::Lay[])
    unbounded = branches.reduce(:&).reduce(order_collector).call(Berylx::Lay[])
    deep_bounded = branches.reduce(:&).workers(2).call(Berylx::Lay[])

    assert_equal [1, 2, 3, 4], bounded.focus.to_h[:order]
    assert_equal unbounded.focus.to_h, bounded.focus.to_h
    assert_equal({ id: 4 }, deep_bounded.focus.to_h)
  end

  # pin: workers.short_circuit
  def test_workers_short_circuit_returns_first_err_in_definition_order
    ran = Thread::Queue.new
    workflow = build_short_circuit_branches(ran).reduce(:&).workers(2).short_circuit

    result = workflow.call(Berylx::Lay[])

    assert_instance_of Berylx::Err, result
    assert_equal :err_first, result.code
    assert_equal :slow_first, result.failed_node
    assert_equal({ from: :first }, result.focus.to_h)
    assert_empty result.parallel_errors
    assert_equal %i[fast_second queued_third slow_first], drain(ran).sort
  end

  # pin: workers.accumulate
  def test_workers_accumulate_collects_all_errs_in_definition_order
    b1_done = Thread::Queue.new
    branches = [
      Berylx::Task[:err0] do |lay|
        b1_done.pop
        lay.reject(:code0, 'zero')
      end,
      Berylx::Task[:ok1] do |lay|
        b1_done << true
        lay[:ok].set(true)
      end,
      Berylx::Task[:err2] { |lay| lay.reject(:code2, 'two') },
      Berylx::Task[:err3] { |lay| lay.reject(:code3, 'three') }
    ]

    result = branches.reduce(:&).workers(2).accumulate.call(Berylx::Lay[])

    assert_instance_of Berylx::Err, result
    assert_equal :parallel_failed, result.code
    assert_equal :err0, result.failed_node
    assert_equal %i[code0 code2 code3], result.parallel_errors.map(&:code)
    assert_equal %i[err0 err2 err3], result.parallel_errors.map(&:failed_node)
  end

  # pin: workers.exception_start
  def test_workers_exception_start_does_not_start_unstarted_branches
    started = Thread::Queue.new
    fatal = CustomFatal.new('stop now')
    branches = [
      Berylx::Task[:boom] do |_lay|
        started << :boom
        raise fatal
      end,
      record_task(:never1, started),
      record_task(:never2, started)
    ]

    raised = assert_raises(CustomFatal) { branches.reduce(:&).workers(1).call(Berylx::Lay[]) }

    assert_same fatal, raised
    assert_equal %i[boom], drain(started)
  end

  # pin: workers.exception_join / workers.exception_start / workers.exception_raise
  def test_workers_exception_join_joins_started_before_raise_and_skips_unstarted
    fatal = CustomFatal.new('branch 1 fatal')
    release_b0 = Thread::Queue.new
    flags = { b0_finished: false, unstarted_ran: false }
    workflow = build_join_before_raise_branches(fatal, release_b0, flags).reduce(:&).workers(2)
    outcome = Thread::Queue.new
    runner = spawn_catching_runner(workflow, flags, outcome)

    sleep 0.03

    assert_predicate runner, :alive?
    assert_empty outcome

    release_b0 << true
    runner.join
    tag, raised, finished_before_raise = outcome.pop

    assert_equal :raised, tag
    assert_same fatal, raised
    assert finished_before_raise
    refute flags[:unstarted_ran]
  end

  # pin: workers.exception_join / workers.unbounded (無制限でも開始済みを全 join してから再送出)
  def test_workers_unbounded_exception_join_waits_for_started_branches
    fatal = CustomFatal.new('unbounded fatal')
    release_b0 = Thread::Queue.new
    flags = { b0_finished: false, unstarted_ran: false }
    branches = build_join_before_raise_branches(fatal, release_b0, flags).first(2)
    outcome = Thread::Queue.new
    runner = spawn_catching_runner(branches.reduce(:&), flags, outcome)

    sleep 0.03

    assert_predicate runner, :alive?
    assert_empty outcome

    release_b0 << true
    runner.join
    tag, raised, finished_before_raise = outcome.pop

    assert_equal :raised, tag
    assert_same fatal, raised
    assert finished_before_raise
  end

  # pin: workers.exception_raise / workers.exception_order / workers.unbounded
  def test_workers_exception_order_reraises_first_raising_branch_in_definition_order
    err0 = CustomFatal.new('definition zero')
    err1 = CustomFatal.new('definition one')

    [2, nil].each do |limit|
      flags = { unstarted_ran: false }
      branches = build_two_raising_branches(err0, err1, flags)
      workflow = Berylx::Parallel.new(branches, workers: limit)
      raised = assert_raises(CustomFatal) { workflow.call(Berylx::Lay[]) }

      assert_same err0, raised
      refute flags[:unstarted_ran] if limit == 2
    end
  end

  # pin: workers.exception_order (3 枝: 定義順 join の実測と、b0 成功遅い / b1 例外遅い / b2 例外速い → b1 が勝つ)
  def test_workers_exception_order_with_ok_earlier_branch_and_two_raising_branches
    err1 = CustomFatal.new('branch 1')
    err2 = CustomFatal.new('branch 2')
    flags = { b0_finished: false, b3_ran: false }
    join_order = []
    branches = build_three_branch_exception_order(err1, err2, flags, join_order, Mutex.new)

    raised = assert_raises(CustomFatal) { branches.reduce(:&).workers(3).call(Berylx::Lay[]) }

    assert_same err1, raised
    assert_equal [0, 1, 2], join_order
    assert flags[:b0_finished]
    refute flags[:b3_ran]
  end

  # pin: workers.flatten_limited / result.parallel_default
  def test_workers_flatten_limited_does_not_leak_inner_on_err_or_reducer_to_outer
    a = set_task(:a, :a, 1)
    b = set_task(:b, :b, 2)
    c = set_task(:c, :c, 3)
    inner = (a & b).reduce(Berylx::Merge.keep_left).accumulate.workers(1)
    outer = inner & c

    assert_equal :short_circuit, outer.on_err
    assert_nil outer.workers
    assert_equal({ c: 3 }, outer.call(Berylx::Lay[]).focus.to_h)

    fa = Berylx::Task[:fa] { |lay| lay.reject(:ea, 'a') }
    fb = Berylx::Task[:fb] { |lay| lay.reject(:eb, 'b') }
    fc = Berylx::Task[:fc] { |lay| lay.reject(:ec, 'c') }
    err_outer = (fa & fb).accumulate.workers(1) & fc
    err_result = err_outer.call(Berylx::Lay[])

    assert_equal :short_circuit, err_outer.on_err
    assert_equal :parallel_failed, err_result.code
    assert_equal %i[ea eb], err_result.parallel_errors.map(&:code)
  end

  # pin: workers.flatten_limited
  def test_workers_flatten_limited_retains_limited_node_as_single_branch
    a = set_task(:a, :a, 1)
    b = set_task(:b, :b, 2)
    c = set_task(:c, :c, 3)
    d = set_task(:d, :d, 4)

    limited = (a & b).workers(1)
    left_outer = limited & c
    right_outer = c & limited
    both_limited = limited & (c & d).workers(2)
    via_new = Berylx::Parallel.new([limited, c], workers: 2)

    assert_equal [limited, c], left_outer.branches
    assert_nil left_outer.workers
    assert_equal [c, limited], right_outer.branches
    assert_nil right_outer.workers
    assert_equal 2, both_limited.branches.size
    assert_equal [1, 2], both_limited.branches.map(&:workers)
    assert_equal [limited, c], via_new.branches
    assert_equal 2, via_new.workers
  end

  # pin: workers.flatten_unlimited
  def test_workers_flatten_unlimited_flattens_into_outer_parallel
    a = set_task(:a, :a, 1)
    b = set_task(:b, :b, 2)
    c = set_task(:c, :c, 3)
    d = set_task(:d, :d, 4)

    flat_and = (a & b) & (c & d)
    flat_new = Berylx::Parallel.new([a & b, c], workers: 2)
    mixed = (a & b).workers(1) & (c & d)

    assert_equal [a, b, c, d], flat_and.branches
    assert_equal [a, b, c], flat_new.branches
    assert_equal 2, flat_new.workers
    assert_equal 3, mixed.branches.size
    assert_instance_of Berylx::Parallel, mixed.branches[0]
    assert_equal [c, d], mixed.branches[1..]
  end

  # pin: workers.nested
  def test_workers_nested_limits_apply_multiplicatively_per_level
    tracker = ConcurrencyTracker.new
    wave = Barrier.new(4) # outer(2) * inner(2) = 4 同時実行
    groups = (0...3).map { |group| build_nested_group(group, tracker, wave) }

    result = groups.reduce(:&).workers(2).call(Berylx::Lay[])

    assert_instance_of Berylx::Ok, result
    assert_equal 4, tracker.peak
    assert_equal 9, result.focus.to_h.size
  end

  # pin: workers.pool
  def test_workers_pool_is_not_globally_shared
    barrier = Barrier.new(2)
    wf_a = (barrier_task(:a1, barrier) & set_task(:a2, :a2, 2)).workers(1)
    wf_b = (barrier_task(:b1, barrier) & set_task(:b2, :b2, 2)).workers(1)

    t1 = Thread.new { wf_a.call(Berylx::Lay[]) }
    t2 = Thread.new { wf_b.call(Berylx::Lay[]) }

    assert_equal({ a1: 1, a2: 2 }, t1.value.focus.to_h)
    assert_equal({ b1: 1, b2: 2 }, t2.value.focus.to_h)

    inner_barrier = Barrier.new(2)
    inner = (barrier_task(:i1, inner_barrier) & barrier_task(:i2, inner_barrier)).workers(2)
    nested = (inner & set_task(:after, :after, 3)).workers(1)

    assert_equal({ i1: 1, i2: 1, after: 3 }, nested.call(Berylx::Lay[]).focus.to_h)
  end

  # pin: workers.documentation
  def test_workers_documentation_explains_ruby_gvl_and_wait_heavy_work
    readme = File.read(File.expand_path('../README.md', __dir__))

    assert_match(/^## Parallel workers\b/, readme)
    assert_match(/GVL/, readme)
    assert_match(/external command/i, readme)
    assert_match(/network/i, readme)
  end

  # pins: substrate.effect_tree / substrate.parallel.mapped / migration.legacy_removed
  def test_workers_executes_through_effect_tree_substrate_and_aspects
    a, b, c = %i[a b c].map { |name| Berylx::Task[name] { |lay, io| lay[name].set(io.perform(:fetch, name)) } }
    workflow = ((a & b).workers(1) & c).workers(2)
    effect = Berylx::EffectTree.build(workflow, {})

    seen = Thread::Queue.new
    handlers = Berylx::EffectTree.around(fetch: ->(key) { :"val_#{key}" }) do |tag, payload, inner|
      seen << tag
      inner.call(payload)
    end
    result = Berylx::EffectTree.run(workflow, Berylx::Lay[], handlers: handlers)
    tags = drain(seen)
    dry = Berylx::EffectTree.dry_run(workflow, {})

    assert_equal Berylx::EffectTree::PARALLEL, effect.tag
    assert_equal 2, effect.payload[0].workers
    assert_equal({ a: :val_a, b: :val_b, c: :val_c }, result.focus.to_h)
    assert_equal 2, tags.count(Berylx::EffectTree::PARALLEL)
    assert_equal 3, tags.count(Berylx::EffectTree::TASK)
    assert_equal 3, tags.count(:fetch)
    assert_equal %i[a b c], dry.steps
  end

  private

  def barrier_task(name, barrier)
    Berylx::Task[name] do |lay|
      barrier.wait
      lay[name].set(1)
    end
  end

  def record_task(name, queue)
    Berylx::Task[name] do |lay|
      queue << name
      lay
    end
  end

  def spawn_catching_runner(workflow, flags, outcome)
    Thread.new do
      workflow.call(Berylx::Lay[])
      outcome << :returned
    rescue CustomFatal => e
      outcome << [:raised, e, flags[:b0_finished]]
    end
  end
end
