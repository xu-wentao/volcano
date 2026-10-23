# Volcano DRA 节点打分设计（修订版）

## 1. 结论

Volcano 当前确实缺少 DRA 打分，但需要区分两个不同问题：

1. **语义完整性问题**：Volcano 已复用 kube-scheduler 的 `dynamicresources` 插件执行
   `PreFilter`、`Filter`、`Reserve` 和 `PreBind`，却没有接入该插件已经实现的
   `Score` 和 `NormalizeScore`。这会丢失 `FirstAvailable` 请求的优先级语义。
2. **调度策略问题**：kube-scheduler 原生 DRA Score 不做设备利用率 binpack；
   Volcano 如需减少设备碎片，还要单独增加一种 DRA binpack 策略。

本设计按两个阶段落地：

- **阶段一（必须先完成）**：在 predicates 插件中完整接入 kube-scheduler 原生 DRA
  `Score/NormalizeScore`。该阶段可直接实施，也是
  [Volcano issue #5329](https://github.com/volcano-sh/volcano/issues/5329) 的目标。
- **阶段二（有前置条件）**：基于 DRA Filter 为每个候选节点产生的**实际分配方案**
  计算设备利用率。当前 Kubernetes 插件把该方案保存在私有 CycleState 中，Volcano
  没有受支持的读取接口；在解决这个接口问题前，不应实现启发式 DRA binpack。

原方案提出的 `DeviceClass -> driver` 猜测、session 级 ResourceSlice 计数缓存和
`AllocateFunc/DeallocateFunc` 手工增减不能作为实现基础。它们会在多 driver、
选择器、共享 Claim、`AllNodes`、多 slice generation、consumable capacity 和
gang 回滚等场景中产生错误结果。

## 2. 当前实现基线

本设计以当前 `go.mod` 和 `vendor/modules.txt` 声明的 Kubernetes `v1.36.1` 为
语义基线。后续升级 Kubernetes 依赖时，需要用 extension-point guard 重新确认
`dynamicresources` 实现的接口和 Volcano 的适配范围。

Volcano 当前链路如下：

| 阶段 | 当前实现 | 结论 |
| --- | --- | --- |
| Pod/Claim 摘要 | `pkg/scheduler/cache/cache.go` 的 `buildTaskDRAInfo` | 只适合 capacity quota 摘要，不足以描述物理分配 |
| PreFilter/Filter | predicates 复用 `dynamicresources.DynamicResources` | 已接入 |
| Score/NormalizeScore | predicates 未注册 DRA Score | 缺失 |
| Reserve/Unreserve/PreBind | predicates 复用原生 DRA 插件 | 已接入 |
| CPU/内存等 binpack | `pkg/scheduler/plugins/binpack/binpack.go` | 只处理 `TaskInfo.Resreq` 标量资源 |

`task.DRAResreq` 目前只保留 `DeviceClass`、Count 和 capacity 维度，并且会跳过
`FirstAvailable`、`allocationMode: All` 等请求。它还没有保留 request selector、
toleration、constraint、request/subrequest 名称和最终设备身份。因此它可以用于
“这个队列逻辑上申请了多少资源”的 quota 统计，不能回答“这个节点实际会分到哪些
设备”。

## 3. Kubernetes DRA 的打分语义

### 3.1 资源身份与可访问性

DRA 设备身份是 `(driver, pool, device)`，不是 `DeviceClass`。`DeviceClass` 只有
selectors，可以匹配一个或多个 driver，也不存在通用的 `DeviceClass -> driver`
一一映射约定。

ResourceSlice 的 pool 可能：

- 分布在多个 slice 中；
- 通过 `generation` 和 `resourceSliceCount` 共同描述一个完整快照；
- 使用 `nodeName`、`nodeSelector`、`allNodes` 或 per-device node selection
  描述可访问性；
- 通过 shared counters 表达 partitionable/consumable capacity；
- 受 DeviceTaintRule、设备状态和请求 toleration 影响。

所以，按 class 或 driver 简单统计 `len(slice.Spec.Devices)` 不是可靠的节点容量。

### 3.2 原生调度链路

Kubernetes `v1.36.1` 的 `dynamicresources` 插件按以下顺序工作：

```text
PreFilter
  ├─ 解析 Pod / PodGroup ResourceClaim
  ├─ 读取 DeviceClass、ResourceSlice、已分配和 pending allocation
  └─ 创建 structured allocator
        ↓
Filter(node)
  ├─ 对该节点执行真实的设备选择
  └─ 将该节点的 AllocationResult 保存到同一个 CycleState
        ↓
Score(node)
  └─ 从该节点的 AllocationResult 判断命中了哪个 FirstAvailable subrequest
        ↓
NormalizeScore
  └─ 归一化到 [0, MaxNodeScore]
        ↓
Reserve(node)
  ├─ 取出所选节点的 AllocationResult
  └─ 通过 DRAManager 记录 pending allocation
        ↓
PreBind
  └─ 持久化 ResourceClaim allocation
```

原生 Score 的含义是“优先满足 `FirstAvailable` 列表中靠前的 subrequest”，不是
“优先选择设备利用率高的节点”。对于只有 ExactCount 的普通 Claim，原生 Score
通常为 0。

## 4. 设计目标与非目标

### 4.1 目标

1. 保持 Volcano 与当前 kube-scheduler DRA Score 语义一致。
2. Score 必须复用同一调度周期中 PreFilter/Filter 写入的 CycleState。
3. 正确调用插件自己的 `ScoreExtensions()` 和 `NormalizeScore`。
4. 为未来的 DRA binpack 明确安全的数据边界和最小首版范围。
5. 不改变 capacity plugin 的 queue quota 语义。

### 4.2 非目标

- 不在 Volcano 中复制 Kubernetes structured allocator。
- 不解析 CEL 文本来猜测 driver 或设备集合。
- 不用 `task.DRAResreq` 替代物理设备分配结果。
- 不在首版支持 consumable capacity、partitionable device 或共享容量 binpack。
- 不通过 weight 为 0 表示 spread；weight 为 0 只表示关闭该打分项。

## 5. 阶段一：接入原生 DRA Score

### 5.1 注册位置

DRA Score 应继续由 predicates 插件中已经创建的同一个
`dynamicresources.DynamicResources` 实例执行，不能在 binpack 或 nodeorder 中
重新创建第二个实例。

```text
predicates.OnSessionOpen
  ├─ PrePredicateFn  -> DynamicResources.PreFilter
  ├─ PredicateFn     -> DynamicResources.Filter
  ├─ BatchNodeOrderFn
  │    ├─ DynamicResources.Score(each candidate)
  │    └─ DynamicResources.NormalizeScore(all candidates)
  └─ AllocateFunc    -> DynamicResources.Reserve
```

必须使用 `ssn.GetCycleState(task.UID)`：Filter 生成的每节点 AllocationResult 和
Score 读取的 state 必须是同一个对象。`BatchNodeOrderFn` 只接收已经通过 Filter
的候选节点，符合 NormalizeScore 需要在完整候选集合上运行的要求。

### 5.2 score helper 调整

当前 `nodescore.BaseScorePlugin` 强制插件同时实现 `PreScorePlugin` 和
`ScorePlugin`，而 Kubernetes DRA 只实现 `ScorePlugin`。公共 score helper 需要：

1. 接收 `fwk.ScorePlugin`；
2. 仅当插件同时实现 `fwk.PreScorePlugin` 时才调用 `PreScore`；
3. 直接读取 `plugin.ScoreExtensions()`；
4. 当 `ScoreExtensions()` 非 nil 时执行 `NormalizeScore`；
5. 检查并返回 NormalizeScore 的失败状态，不能像当前实现一样忽略；
6. 归一化完成后再校验 `[MinNodeScore, MaxNodeScore]` 并应用 Volcano 权重。

这样既支持没有 PreScore 的 DRA，也能删掉各调用方维护 `EmptyNormalizer` 或手工
指定 normalizer 的重复逻辑。

### 5.3 配置

建议在 predicates arguments 中增加一个与 `volumebinding.weight` 对称的参数：

```yaml
tiers:
- plugins:
  - name: predicates
    arguments:
      predicate.DynamicResourceAllocationEnable: true
      dynamicresources.filterTimeoutSeconds: 10
      dynamicresources.weight: 1
```

语义：

- 默认值为 `1`，与 kube-scheduler 默认 profile 的常见权重一致；
- `0` 只关闭 DRA Score，不关闭 DRA Filter、Reserve 或 PreBind；
- 负数属于非法配置，应在初始化时明确报错，不能解释成 spread。

这个权重只影响 `FirstAvailable` 偏好。ExactCount 请求的原生 DRA Score 为 0，
调整该权重不会产生设备 binpack 效果。

### 5.4 错误处理

- PreScore、Score、NormalizeScore 任一返回失败时，该次 BatchNodeOrder 必须返回
  错误，不使用部分节点分数。
- 插件返回越界分数时沿用现有 score helper 的校验并报错。
- 没有 DRA Claim、没有候选 AllocationResult 或插件因 feature gate 未启用时返回
  0，不能把这类正常情况记为调度错误。
- 不得吞掉 DeviceClass、ResourceSlice 或 ResourceClaim lister 错误。

## 6. 阶段二：DRA 设备利用率 binpack

### 6.1 前置阻塞

Filter 已经计算了候选节点的真实 AllocationResult，但
`dynamicresources.stateData.nodeAllocations` 是 Kubernetes 包内私有字段。
Volcano 当前无法在不复制 allocator、不依赖反射的情况下安全读取它。

进入实现前，必须先获得一个受支持的只读契约。优先顺序是：

1. 推动 Kubernetes DRA 插件暴露只读的候选分配/评分扩展点；
2. 若上游暂不接受，在 Volcano 的 Kubernetes 依赖适配层维护一个最小、可测试的
   hook，并明确跟随 Kubernetes 版本升级；
3. 不接受复制 structured allocator、读取私有 CycleState、反射或重新扫描后猜测
   分配结果的方案。

该契约至少要提供：

- Claim UID 和 request/subrequest 身份；
- 候选节点实际选中的 `(driver, pool, device)`；
- 该结果是复用已有 allocation，还是会产生新 allocation；
- 对当前请求和节点真正 eligible 的设备集合或等价容量统计；
- consumable capacity 若未来支持时的已消耗、pending 和本次新增容量。

在此契约完成前，阶段二状态为**不可实施**，但不阻塞阶段一。

### 6.2 首版支持范围

即使获得候选分配契约，首版也只支持：

- `allocationMode: ExactCount`；
- 独占、整设备分配；
- `ResourceSlice.spec.nodeName` 表达的 node-local 设备；
- 完整且 generation 一致的 pool 数据；
- Filter 已成功产生的新 allocation。

以下情况整个 task 的 DRA binpack 返回 0，由原生 DRA 逻辑继续决定可行性和
`FirstAvailable` 偏好：

- `FirstAvailable`（避免 binpack 覆盖用户显式 subrequest 优先级）；
- `allocationMode: All`；
- 已分配或被多个 Pod 共享的 Claim；
- consumable capacity、shared counters、partitionable device、ShareID；
- `allNodes`、`nodeSelector`、per-device node selection；
- DRA Extended Resource 生成的隐式 Claim，除非候选分配契约明确暴露它；
- pool 快照不完整、generation 不一致或任何身份无法确认的情况；
- 同一个 task 混合了支持和不支持的 DRA 请求，避免只对部分请求打分造成偏置。

这是有意的安全降级：Filter/Reserve 仍保持完整的 DRA 正确性，缺少的只是一个
可选排序偏好。

### 6.3 打分公式

对候选节点 `n`，按候选分配结果涉及的 eligible device domain `d` 计算：

- `T_d(n)`：该请求在该节点可使用的独占设备总数；
- `U_d(n)`：其中已分配或由 DRAManager 标记 pending 的设备数；
- `R_d(n)`：该候选分配方案将新增的设备数。

```text
util_d(n) = (U_d(n) + R_d(n)) / T_d(n)

draScore(n) =
    100 * sum(R_d(n) * util_d(n)) / sum(R_d(n))
```

只有 `0 < U_d + R_d <= T_d` 时该 domain 才可打分。`R_d` 作为权重，使一次申请多个
设备时不会与单设备申请等价。

最终 binpack 分数：

```text
finalBinpackScore(n) =
    currentScalarBinpackScore(n)
    + binpack.dra.weight * draScore(n)
```

其中 `currentScalarBinpackScore` 已包含现有 `binpack.weight`。DRA 分数自身保持
`[0, 100]`，只应用一次 `binpack.dra.weight`，不能先乘权重再除以同一个权重。

建议配置：

```yaml
- name: binpack
  arguments:
    binpack.weight: 10
    binpack.cpu: 5
    binpack.memory: 1
    binpack.dra.weight: 0  # 阶段二默认关闭；显式设为正数后启用
```

阶段二应使用 `BatchNodeOrderFn`，因为它需要一次看到完整候选集及同一个
CycleState。现有 CPU/内存 NodeOrderFn 可以保留。

### 6.4 gang、Pipeline 和回滚

DRA 资源状态必须以原生 DRAManager 为准：

- `Reserve` 会把所选 AllocationResult 记录为 pending allocation；
- 后续 task 的 PreFilter/Filter 会看到 pending allocation；
- `Unreserve` 负责在失败或回滚时释放相应 pending 状态；
- 已持久化 allocation 由 ResourceClaim status 表达。

binpack 插件不得再用 `AllocateFunc/DeallocateFunc` 维护 `used++/used--`。原因是
Volcano 的 `Statement.Pipeline` 也会调用 `AllocateFunc`，dry-run discard/recover、
preempt、reclaim 和共享 Claim 还会重复触发不同事件；事件次数不等于物理设备
allocation 次数。

当前 Pipeline 会进一步触发 predicates 的 DRA Reserve。这个生命周期是否符合
Pipeline 的长期语义，需要作为 predicates/statement 的独立问题验证；不能靠 score
缓存“修正”。

Kubernetes `v1.36` 对 PodGroup 共享 pending ResourceClaim 有专门的 scheduling
cycle 状态。Volcano 的 gang/PodGroup 与 Kubernetes `Pod.spec.schedulingGroup`
并非天然等价。在宣称支持“多个 gang task 共享未分配 Claim”之前，必须增加集成
验证；普通的 Volcano Allocate 事件不能替代该语义。

### 6.5 并发与缓存

Volcano 会并发计算多个节点分数，因此 DRA Score 路径只能读取本 task 的
CycleState 和线程安全的 DRA manager 快照，不能在 Score 中修改 session 级 map。

不新增第二套 session 级 ResourceSlice 缓存。Kubernetes DRA manager/allocator 已经
负责 informer 更新、pool generation、设备 taint 和 pending allocation；复制一份
缓存不仅重复，还会在 session 内对象变化时陈旧。

## 7. 与 capacity DRA 支持的关系

两者职责不同，但数据和生命周期不能被认为“完全正交”：

| 维度 | capacity DRA | DRA Score |
| --- | --- | --- |
| 目标 | queue 逻辑 quota | 候选节点排序 |
| 主要输入 | Claim 请求摘要 | Filter 的实际候选 AllocationResult |
| 资源粒度 | `deviceclass/<name>` 和 capacity dimension | `(driver, pool, device)` 及实际 capacity |
| 共享 Claim | quota 层应按 Claim UID 去重 | 复用 allocation，不重复增加物理使用量 |
| 正确性边界 | 不决定物理可分配性 | 不决定 queue 是否可用 |

`task.DRAResreq` 可以继续供 capacity 使用，但不得成为阶段二的物理设备数据源。
两项功能可以同时开启；任何一项都不能假设另一项已经验证了 DRA 请求的全部语义。

## 8. 预计改动范围

### 8.1 阶段一

| 文件 | 改动 |
| --- | --- |
| `pkg/scheduler/plugins/util/nodescore/score_helper.go` | 支持无 PreScore 的 ScorePlugin，使用插件自身 normalizer，并检查状态 |
| `pkg/scheduler/plugins/util/nodescore/score_helper_test.go` | 覆盖 optional PreScore、NormalizeScore 成功/失败和越界 |
| `pkg/scheduler/plugins/predicates/helper.go` | 增加 `dynamicresources.weight` |
| `pkg/scheduler/plugins/predicates/predicates.go` | 将同一 DRA 实例注册为 ScorePlugin |
| `pkg/scheduler/plugins/predicates/predicates_test.go` | 覆盖注册、权重、CycleState 复用和候选集 |
| DRA e2e 测试 | 验证不同节点命中不同 FirstAvailable subrequest 时的排序 |
| kube 插件接口 guard 测试 | kube 依赖升级后发现新增但未处理的 extension point |

`nodeorder` 也使用公共 score helper。如果 helper 删除了外部 normalizer 参数，应同步
简化 `scorePluginEntry` 并运行 nodeorder 回归测试。

### 8.2 阶段二

阶段二的文件范围取决于候选分配只读契约放在哪里。在该契约评审通过前，不承诺
“只改 binpack.go 和 binpack_test.go”，也不应先新增 DRA session cache。

## 9. 测试矩阵

### 9.1 阶段一必测

| 场景 | 期望 |
| --- | --- |
| 无 DRA Claim | 所有节点 DRA Score 为 0 |
| ExactCount | 不改变节点排序 |
| 两节点命中不同 FirstAvailable subrequest | 命中较早 subrequest 的节点得分更高 |
| 多个 Claim 各有 FirstAvailable | 分数正确累加并归一化 |
| Filter 后只剩一个节点 | NormalizeScore 正常，Reserve 使用同一节点方案 |
| ScoreExtensions 为 nil | 不调用 NormalizeScore |
| NormalizeScore 返回错误 | BatchNodeOrder 返回错误，不使用部分结果 |
| `dynamicresources.weight: 0` | 只关闭 Score，Filter/Reserve/PreBind 仍执行 |
| Reserve 后调度下一 task | 下一 task 能看到 pending allocation |
| Statement discard/recover | pending allocation 被正确 Unreserve |
| 共享未分配 Claim 的 gang tasks | 未完成 schedulingGroup 集成前不得误报支持 |

### 9.2 阶段二额外必测

| 场景 | 期望 |
| --- | --- |
| 两个 node-local pool 利用率不同 | binpack 选择 post-allocation 利用率更高者 |
| 同一请求一次分配多个设备 | `R_d` 正确加权 |
| DeviceClass 匹配多个 driver | 使用实际 AllocationResult，不猜 driver |
| 已分配/共享 Claim | DRA binpack 为 0，不重复计数 |
| FirstAvailable、All 或 consumable capacity | 整个 task 降级为 0 |
| pool generation 不完整 | 降级为 0 并记录可观测原因 |
| 并发 Score | race test 无共享 map 读写竞争 |
| Pipeline、discard、preempt、reclaim | 不存在独立缓存漂移 |

## 10. 发布与兼容性

1. 先合入阶段一和 extension-point guard，恢复与 kube-scheduler 的语义一致性。
2. 在小规模 DRA 集群验证 FirstAvailable、Reserve/Unreserve 和 gang 行为。
3. 单独评审候选 AllocationResult 的只读契约。
4. 阶段二以 `binpack.dra.weight: 0` 默认关闭的方式发布。
5. 只有支持范围内场景通过 e2e 和 race 测试后，才考虑扩大到 global、
   consumable capacity 或 partitionable device。

## 11. 参考资料

- [Volcano issue #5329: Support DRA Score/NormalizeScore](https://github.com/volcano-sh/volcano/issues/5329)
- [Kubernetes v1.36.1 DynamicResources plugin](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go)
- [Kubernetes DRA concepts](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
- [Kubernetes DeviceClass API](https://kubernetes.io/docs/reference/kubernetes-api/resource/device-class-v1/)
- [Kubernetes ResourceSlice API](https://kubernetes.io/docs/reference/kubernetes-api/resource/resource-slice-v1/)
- [Kubernetes ResourceClaim API](https://kubernetes.io/docs/reference/kubernetes-api/resource/resource-claim-v1/)
- [Volcano capacity DRA design](./capacity-dra-support.md)
- [Capacity DRA support review](./capacity-dra-support-review.md)（quota，与本文打分无关）

## 12. 相对 Kubernetes v1.36.1 的实现状态

本节记录本次落地时核对过的代码，避免把设计基线写成已经不存在的缺口。

### 12.1 阶段一

`predicates` 已经用同一个 `*dynamicresources.DynamicResources` 实例注册
`PreFilter`、`Filter`、`Reserve`、`PreBind` 和 `Score`。`BatchNodeOrder` 读取
`ssn.GetCycleState(task.UID)`，与 PrePredicate / Predicate 相同。
`nodescore.CalculatePluginScore` 会调用插件自己的 `ScoreExtensions().NormalizeScore`，
失败则整批返回错误；归一化之后才检查 `[MinNodeScore, MaxNodeScore]` 并乘上
Volcano 权重。`DynamicResources` 没有 `PreScore`，调用方只在插件同时实现
`PreScorePlugin` 时才调用 `PreScore`。

本次补上的配置是 `dynamicresources.weight`：

- 缺省为 `1`；
- `0` 不注册 DRA Score，Filter / Reserve / PreBind 仍执行；
- 负数或非整数在初始化时失败，不能解释成 spread。

原生 Score 仍然只表达 `FirstAvailable` 偏好。ExactCount 的原生分数通常为 0。
调整该权重不会产生设备利用率 binpack。Kubernetes v1.36 把
`DynamicResourceAllocation` 锁为开启；插件自身在 `enabled == false` 时仍对
每个节点返回 0，且 `NormalizeScore` 返回 nil。这个 nil 状态按成功处理，不会
被记成调度错误。没有 Filter 写入的 CycleState 时，Score 失败会使整批
BatchNodeOrder 失败。

扩展点 guard 在 `pkg/scheduler/plugins/predicates/dra_extension_points_test.go`。
Kubernetes 给 `DynamicResources` 增加未分类的导出方法时，测试失败，要求明确
写成 adapted 或 ignored。当前明确不接入的点是 `PreEnqueue`、
`EventsToRegister`、`PostFilter`、`SignPod` 和 `PreBindPreFlight`。

### 12.2 阶段二：仍然阻塞

`k8s.io/kubernetes@v1.36.1` 没有可供 Volcano 读取的、Filter 每节点
`AllocationResult` 只读契约。相关符号都在
`pkg/scheduler/framework/plugins/dynamicresources`：

| 符号 | 位置 | 为什么不能当 binpack 输入 |
| --- | --- | --- |
| `stateKey` | `dynamicresources.go` | 包内 `StateKey`，值为 `"DynamicResources"` |
| `stateData.nodeAllocations` | `dynamicresources.go` | 未导出。Filter 写入，Score / Reserve 只在包内读取 |
| `nodeAllocation.allocationResults` | `dynamicresources.go` | 未导出的 `[]resourceapi.AllocationResult` |
| `getStateData` | `dynamicresources.go` | 未导出 |
| `(*DynamicResources).Score` / `computeScore` | `dynamicresources.go` | 只根据命中的 `FirstAvailable` 下标加分，不暴露设备身份 |
| `ExtractPodNodeAllocatableResourceClaimStatus` | `nodeallocatabledynamicresources.go` | 唯一导出的 CycleState 读取函数，返回 `[]v1.NodeAllocatableResourceClaimStatus`，不是设备分配 |
| `(*claimTracker).GetPendingAllocation` | `dra_manager.go` | Reserve 之后的 pending allocation，不是每个候选节点的 Filter 结果 |
| `(*claimTracker).GatherAllocatedState` | `dra_manager.go` | 集群已分配设备，没有本次候选方案，也没有 eligible 设备集合 |

因此还不能得到设计第 6.1 节要求的 Claim UID、request/subrequest、
`(driver, pool, device)`、新分配还是复用、以及 eligible 设备数。

本仓库用 `pkg/scheduler/plugins/util/k8s/dra_candidate_allocation.go` 的
`ReadDRACandidateAllocations` 标出这个缺口。它固定返回
`ErrDRACandidateAllocationUnavailable`，不反射 CycleState，不复制
structured allocator，也不扫描 ResourceSlice。

在上游或 Volcano 的 Kubernetes 适配层提供可测试的只读契约之前：

- 不实现 `binpack.dra.weight`；
- 不把 `task.DRAResreq` 或 `len(slice.Spec.Devices)` 当作节点设备容量；
- 不在 binpack 里用 `AllocateFunc` / `DeallocateFunc` 维护 `used++` / `used--`。

容量插件的 queue quota 见 `capacity-dra-support.md` 与
`capacity-dra-support-review.md`，不在本设计的实现范围内。
