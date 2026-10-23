# Capacity Plugin DRA Support 设计与实现 Review

> 本文只审查 capacity plugin 的 queue quota。它不是 DRA 节点打分或设备利用率
> binpack 的实现说明。打分设计见 [dra-score-design.md](./dra-score-design.md)。
> 本文列出的 quota 修正不在 DRA score 变更里实现。

## 1. Review 范围

本文审查以下内容：

- `docs/design/capacity-dra-support.md`
- DRA 请求摘要生成：
  - `pkg/scheduler/cache/cache.go`
  - `pkg/scheduler/cache/event_handlers.go`
  - `pkg/scheduler/api/job_info.go`
- capacity plugin 的 queue admission、allocation、层级队列和共享 Claim 记账：
  - `pkg/scheduler/plugins/capacity/capacity.go`
- Queue `status.allocated` 的 DRA 展示：
  - `pkg/scheduler/framework/session.go`
  - `pkg/scheduler/framework/session_dra_queue_status.go`
- 单元测试和 E2E：
  - `pkg/scheduler/cache/cache_dra_test.go`
  - `pkg/scheduler/plugins/capacity/capacity_test.go`
  - `pkg/scheduler/framework/session_dra_queue_status_test.go`
  - `test/e2e/dra/dra_quota_test.go`
- Kubernetes v1.36.1 的 DRA API 和 scheduler `DynamicResources` 实现。

本文区分四类结论：

- **correctness bug**：当前行为会违反已经声明的配额语义；
- **实现缺口**：设计宣称支持，但代码尚未接入对应 capacity 机制；
- **设计/契约问题**：必须先明确语义，否则无法判断实现是否正确；
- **明确限制**：可以接受，但必须显式拒绝或清楚暴露，不能静默放行。

## 2. 总体结论

“用 Queue `ResourceList` 表达 DRA 逻辑配额、由 Kubernetes DRA 继续负责物理分配”这个职责划分是合理的；把第一版收窄到按 `DeviceClass` 的 `ExactCount` 硬上限也可以形成可用的最小方案。

但当前设计文档把实际能力描述得过宽。现有实现还不能兑现以下核心承诺：

- DRA 与普通资源具有相同的 `capability`、`deserved`、`guarantee` 语义；
- 所有 DRA 请求都受严格的 queue hard limit 约束；
- shared `ResourceClaim` 在 admission、allocation、层级祖先队列和 Queue Status 中都只计一次；
- DRA inqueue 预留能跨 scheduling session 保持；
- consumable capacity 表示 Kubernetes 实际消耗的 capacity；
- root queue 的 DRA total 可以自动来自集群状态。

当前真正形成闭环的范围更接近：

> Pod 显式引用 `ResourceClaim` 或 Pod 级 `ResourceClaimTemplate`，Claim 只包含 `Exactly + ExactCount` 请求；按请求中声明的 `DeviceClass` 和原始 capacity 数量，在单个 queue 内对已分配 Task 做硬上限检查，并对同名 Claim 做部分去重。

即使在这个窄范围内，仍存在共享 Claim admission 重复计费、inqueue 跨 session 丢失、层级祖先队列重建漏计、`capability: 0` 被当成无限制等 correctness bug。因此不建议把当前实现描述为可提供严格 DRA queue quota 或与普通 capacity 资源完全等价。

## 3. 设计承诺与当前实现对照

| 能力 | 文档声明 | 当前实现 | 结论 |
|---|---|---|---|
| `capability` device count | hard upper bound | `checkDRAAllocatable` 单独检查 | 部分可用，但有绕过和边界 bug |
| `capability` capacity dimension | hard upper bound | 检查 Claim 中的原始请求量 | 契约不清；不是实际 `ConsumedCapacity` |
| `deserved` | soft share / borrow / reclaim | 只解析，DRA allocated 未参与 share/reclaim | 未实现 |
| `guarantee` | minimum protected share | 只解析，DRA usage 未参与保护或回收 | 未实现 |
| direct Claim | 支持 | 正常路径支持 | informer race 会丢 Task |
| Pod 级 ClaimTemplate | 支持 | resolved Claim 支持 | 正常路径基本成立 |
| shared Claim | 每个 Claim 一次 | allocation 局部去重 | enqueue、跨 queue/祖先重建不一致 |
| `FirstAvailable` | deferred | 静默跳过 | 会绕过 hard quota |
| `allocationMode: All` | deferred | 静默跳过 | 会绕过 hard quota |
| DRA Extended Resource | 未说明 | 完全不进入摘要 | v1.36 beta 能力未覆盖 |
| Kubernetes Workload/PodGroup Claim | 未说明 | 未接入 native PodGroup cycle state | v1.36 alpha 能力未覆盖 |
| root DRA total | cluster state 或显式配置 | 只有显式 Queue 配置 | 文档与实现不符 |

## 4. 按严重级别排序的问题

### 4.1 Critical：unsupported 请求被静默跳过，可绕过 hard quota

类型：**correctness bug + 不安全的限制处理**

证据：

- 设计把 `capability` 定义为 hard upper bound：`docs/design/capacity-dra-support.md:42-52`。
- `FirstAvailable` 和 `allocationMode: All` 只是被描述为“不是主要目标”：`docs/design/capacity-dra-support.md:164-172`。
- cache 对两类请求只打印 warning 后 `continue`：
  - `pkg/scheduler/cache/cache.go:1973-1977`
  - `pkg/scheduler/cache/cache.go:1982-1985`
- 如果一个 Claim 全部是上述请求，`buildTaskDRAInfo` 最终返回 nil：`pkg/scheduler/cache/cache.go:2007-2024`。
- capacity 在 `taskDRA == nil` 时直接放行：`pkg/scheduler/plugins/capacity/capacity.go:246-249`。

影响：

- 一个 queue 配置了 `deviceclass/gpu.example.com: "1"`，用户可以改用 `FirstAvailable` 或 `All` 请求同一类设备，Volcano 会把请求视为没有 DRA quota consumption。
- 混合 Claim 中只有支持的 request 被计费，其余 request 仍然绕过 quota。
- 这是 silent fail-open，不符合 hard upper bound 的安全边界。

建议：

- 第一版若不支持这些模式，应在 queue 启用了相关 DRA quota 时明确拒绝，并给 PodGroup/Job 记录可诊断事件；
- cache 不能把“不支持”压缩成“无 DRA 请求”，至少要保留 unsupported reason 和涉及的 DeviceClass；
- 长期支持 `FirstAvailable` 时，计费必须跟随 Kubernetes Filter 选中的 subrequest；不能把所有备选项相加，也不能固定取第一个；
- `All` 的数量依赖匹配设备集合，不能仅从 Claim Spec 得出，应基于实际 allocation result 或明确不支持。

### 4.2 Critical：`deserved` / `guarantee` 并未实现，且可能扭曲 queue 排序

类型：**实现缺口 + correctness bug**

证据：

- 文档声明三者语义与普通资源完全相同：`docs/design/capacity-dra-support.md:42-52`、`174-181`。
- DRA-specific `deserved` 和 `guarantee` 被解析进 `draQuotaAttr`：`pkg/scheduler/plugins/capacity/capacity.go:103-115`、`181-190`。
- 除初始化和 clone 外，没有任何逻辑读取 `dra.deserved` 或 `dra.guarantee`；DRA 检查只使用 `dra.capability`：`pkg/scheduler/plugins/capacity/capacity.go:246-305`。
- queue share 仍只读取普通 `attr.allocated` 和普通 `attr.deserved`：
  `pkg/scheduler/plugins/capacity/capacity.go:1817-1843`。
- DRA Task 只更新 `attr.dra.allocated`，不会把 DRA key 写入普通
  `attr.allocated.ScalarResources`：`pkg/scheduler/plugins/capacity/capacity.go:351-409`、
  `924-1003`。

影响：

- 只配置 DRA `deserved` 的 queue 会在普通 `attr.deserved` 中包含 DRA key，但对应普通 allocated 永远为 0，因此 share 可能长期保持 0，获得不符合实际 DRA 使用量的高优先级。
- DRA 使用量不会参与 queue borrow、reclaim、preemption 或 guarantee 保护。
- 示例中 `deserved` / `guarantee` 的值对 DRA 实际调度没有文档宣称的作用。

建议：

- 最小可交付版本只承诺并允许 DRA `capability`；暂时从设计和用户指南移除 DRA `deserved` / `guarantee` 示例，必要时拒绝这些 key；
- 只有在 DRA count/capacity 被接入 queue dominant share、reclaim victim 判断、guarantee 和 hierarchy 计算后，才能宣称与普通资源相同；
- 不要仅把 DRA key 塞入普通 `Resource` 来“复用”语义，除非 allocation、request、inqueue、elastic 的同名 scalar 也完整维护。

### 4.3 High：shared Claim 在 Job enqueue 阶段仍按 Pod 重复计费

类型：**correctness bug**

证据：

- 文档承诺同一个 shared Claim 只计一次：
  `docs/design/capacity-dra-support.md:134-141`。
- allocation 阶段确实使用 per-queue refcount 去重：
  `pkg/scheduler/plugins/capacity/capacity.go:321-409`。
- 但 `JobInfo.GetMinDRAResources` 只聚合每个 Task 的 `DRAResreq`，完全没有使用
  `ResourceClaimKeys` 或 `ResourceClaimDRAResreq`：
  `pkg/scheduler/api/job_info.go:1492-1584`。
- 没有 `TaskMinAvailable` 时，它按 Task 逐个相加：`pkg/scheduler/api/job_info.go:1525-1552`；
  有 task role 时，它把一个 Task 的请求乘以该 role 的最小副本数：
  `pkg/scheduler/api/job_info.go:1560-1579`。
- shared Claim E2E 有两个副本，但故意使用 `minAvailable=1`：
  `test/e2e/dra/dra_quota_test.go:147-169`，因此没有覆盖 `minAvailable=2`
  时的重复计费。

影响：

- 两个 gang member 引用同一个 count=1 Claim，queue capability=1、minAvailable=2 时，Job enqueue 会计算成 2 并拒绝；而 allocation 和 Queue Status 又只显示 1。
- 同一份配额在 admission 和 allocation 阶段具有不同语义。

建议：

- `GetMinDRAResources` 必须按 Claim identity 聚合，不应先聚合到 Task 后再乘副本数；
- Claim identity 应优先使用 UID，而不是 `namespace/name`，以区分删除后同名重建；
- 对共享 Claim，应明确计费范围：
  - leaf queue：每个 Claim 每个 leaf queue 一次；
  - ancestor/root：同一祖先子树内每个 Claim 一次；
  - 跨 leaf queue 共享时是否每个 leaf 都收费，必须写入契约。

### 4.4 High：DRA inqueue reservation 没有跨 scheduling session 重建

类型：**correctness bug**

证据：

- Job 刚被 admission 时，`AddJobEnqueuedFn` 会把 `GetMinDRAResources` 加入
  `dra.inqueue`：`pkg/scheduler/plugins/capacity/capacity.go:781-813`。
- 新 session 初始化 queue 时，`dra.inqueue` 总是空 map：
  `pkg/scheduler/plugins/capacity/capacity.go:181-188`。
- session open 会从已有 Job 重建普通 `attr.inqueue`，但没有任何对应的
  `updateDRAInqueue`：
  - 非层级队列：`pkg/scheduler/plugins/capacity/capacity.go:1124-1144`
  - 层级队列：`pkg/scheduler/plugins/capacity/capacity.go:1271-1288`
- Task 分配时会增加 `dra.allocated`，但不会从 `dra.inqueue` 扣除已覆盖的最小需求：
  `pkg/scheduler/plugins/capacity/capacity.go:924-963`。
- Task allocatable 检查显式忽略 inqueue，而 Job enqueue 检查会同时计算 allocated 和
  inqueue：`pkg/scheduler/plugins/capacity/capacity.go:1646-1655`、
  `1697-1719`。
- enqueue 检查依赖 `allocated + inqueue + request`：
  `pkg/scheduler/plugins/capacity/capacity.go:1711-1719`。

影响：

- 一个已经 Inqueue、但因物理设备暂不可用而尚未分配的 DRA Job，只在 admission 所在 session 预留 quota；
- 下一个 session 会忘记这份 reservation，后续 Job 可以继续 Inqueue，行为与普通 capacity resource 不一致；
- 同一个 session 内，Job 被 admission 后又分配了部分 Task 时，`dra.allocated` 与完整
  `dra.inqueue` 会同时保留；正常 action 顺序下 Task hard check 不读 inqueue，但如果同一 session
  再执行 enqueue 检查，就会把已覆盖部分重复计算；
- 单个 Task allocation 的 hard check仍可能阻止实际超额，但 queue admission、gang reservation 和公平性已经失真。

建议：

- session open 必须像普通资源一样重建 DRA inqueue；
- 重建值应为“Job 最小 DRA 需求中尚未由已 allocated Claim 覆盖的部分”，并按 Claim 去重，避免同时出现在 allocated 和 inqueue；
- 在 session 内 Job 从 inqueue 转成部分 allocated 时，也应维护同一个“未覆盖 remainder”，不能一边保留完整
  minDRA、一边增加 allocated；
- 增加跨两个 session 的单元测试，不能只直接调用 `updateDRAInqueue` 构造状态。

### 4.5 High：层级 queue 重建会漏计或重复计算祖先 DRA usage

类型：**correctness bug**

证据：

- session open 只在 leaf `attr.dra != nil` 时统计已分配 Task：
  `pkg/scheduler/plugins/capacity/capacity.go:1255-1263`。
- 然后只把 leaf 的数值 delta 传播给配置了同一 DeviceClass capability 的祖先：
  `pkg/scheduler/plugins/capacity/capacity.go:1296-1318`。
- 传播的是聚合数值，没有把 Claim refcount 一起传播。
- 与之相对，session 运行期间的 Allocate/Deallocate event 会直接对每个祖先调用
  `addTaskDRAAllocated` / `removeTaskDRAAllocated`，保留 refcount：
  `pkg/scheduler/plugins/capacity/capacity.go:924-1003`。

影响：

- 父 queue 配置 DRA capability、子 leaf 没配置时，重建 leaf usage 得到 nil，父 queue 看不到 session 开始前已经存在的 DRA allocation；
- 同一个 Claim 被不同 leaf queue 引用时，数值 delta 会在父 queue 重复相加，而 Queue Status 的 root 重算又会按 Claim key 去重；
- session 内增量状态、下一 session 重建状态和 `status.allocated` 三者可能给出不同答案。

建议：

- 不要从 leaf 的聚合 delta 推导祖先 Claim usage；
- session open 应遍历 allocated Task，并在 leaf 及其每个祖先上使用同一套按 Claim identity 的 refcount 逻辑；
- 增加“只有 parent 配 quota”“跨两个 leaf 共享一个 Claim”“session 重开”测试。

### 4.6 High：count-only capability 会意外拒绝带 capacity request 的 Claim

类型：**correctness bug / 契约缺失**

证据：

- `checkDRAAllocatable` 只要启用了 consumable capacity，就遍历请求中的每个 dimension；
- 如果 queue 没配置该 dimension，会直接返回 false：
  `pkg/scheduler/plugins/capacity/capacity.go:282-300`。

影响：

- queue 只想限制设备数量，配置
  `deviceclass/gpu.example.com: "4"`；
- Claim 请求同一 DeviceClass，并附带 `memory: 8Gi`；
- 即使 queue 没有声明 memory quota，也会因为 capability 中没有 memory dimension 而被拒绝。

这与“没有配置某个 quota key 就不限制该维度”的通常 ResourceList 语义不一致，也与代码对未配置 DeviceClass 直接放行的行为不一致。

建议：

- 未配置的 capacity dimension 应视为不限制；
- 如果期望 allowlist 语义，必须在设计中明确说明，并提供独立开关，不能从“出现了 count key”隐式推导。

### 4.7 High：`deviceclass/<class>: "0"` 实际表示“不限制”

类型：**correctness bug**

证据：

- count hard limit 只有 `capability.Count > 0` 时才检查：
  `pkg/scheduler/plugins/capacity/capacity.go:273-280`。
- Queue DRA key 没有整数、非负和零值语义校验：
  `pkg/scheduler/plugins/capacity/capacity.go:142-179`。

影响：

- 管理员把 count capability 设为 0，直觉上表示禁止该 queue 使用设备；
- 实际检查被跳过，变成无限制；
- 负数也会关闭检查；小数经过 `Quantity.Value()` 舍入后的结果不够直观。

建议：

- 0 必须是有效 hard zero；
- 拒绝负数和非整数 count；
- “未配置”与“配置为 0”必须使用 map key 是否存在区分，不能用数值正负区分。

### 4.8 High：direct ResourceClaim informer race 可能导致 Pod 从 cache 丢失

类型：**correctness bug**

证据：

- Pod 到达时，如果 direct Claim 尚未进入 ResourceClaim informer cache，
  `buildTaskDRAInfo` 返回普通 error：`pkg/scheduler/cache/cache.go:1950-1965`。
- `addPod` 只对 `pendingDRAResourceClaimError` 先 `addTask` 再 resync；
  当前只有 template 路径会构造该错误：
  `pkg/scheduler/cache/event_handlers.go:270-280`。
- 普通 error 路径只调用 `resyncTask(pi)`，没有把 Task 加入 `sc.Jobs`：
  `pkg/scheduler/cache/event_handlers.go:298-300`。
- `processResyncTask` 随后必须先从 `sc.Jobs` 找到 Task，找不到就 Forget：
  `pkg/scheduler/cache/cache.go:1224-1263`。

影响：

- Pod ADD 先于 direct ResourceClaim informer ADD 时，这个 Pod 可能不会进入 scheduler cache；
- ResourceClaim 后续到达也没有这里对应的 Task 可重试；
- 现有测试只验证 build 函数返回 error，没有覆盖 `addPod` 的恢复：
  `pkg/scheduler/cache/cache_dra_test.go:227-237`。

建议：

- direct Claim 的 NotFound/informer-not-synced 应与 template race 使用相同的 pending 路径；
- 其他读取错误仍应保留 fail-fast，避免吞掉真实故障；
- 增加与 `TestAddPodWithUnresolvedResourceClaimTemplateCachesTaskForResync`
  对称的 direct Claim 测试。

### 4.9 High：capacity dimension key 无法表达常见的 fully-qualified DRA capacity name

类型：**设计/API 问题**

证据：

- 设计编码为 `<dim>.deviceclass/<DeviceClass>`：
  `docs/design/capacity-dra-support.md:33-40`。
- Kubernetes DRA 的 `QualifiedName` 允许并鼓励第三方 capacity 使用
  `<domain>/<name>`，例如 `dra.example.com/memory`。
- 把该 dimension 代入当前格式会得到
  `dra.example.com/memory.deviceclass/gpu.example.com`，其中包含两个 `/`，
  不能作为 Kubernetes `ResourceName`。
- parser 本身允许 separator 前包含 `/`，但对象会先受到 API ResourceName 格式约束：
  `pkg/scheduler/plugins/capacity/capacity.go:142-163`。

影响：

- 当前 key 只可靠支持无 domain 的简单 dimension；
- 设计声称通用支持 consumable capacity，但无法配置第三方 fully-qualified capacity；
- dimension 与 DeviceClass 的拼接还受 ResourceName 长度限制。

建议：

- 第一版明确限制为 unqualified dimension，并把限制写入 API 文档；或
- 重新设计可逆且合法的 key encoding；
- 如果可读性和完整表达都要求保留，应该重新评估“绝不新增 DRA-specific queue field”这个 non-goal，避免用脆弱字符串编码硬塞进 `ResourceList`。

### 4.10 High：声明的 capacity request 不等于 Kubernetes 实际 consumed capacity

类型：**设计契约问题；若声称物理消耗配额则是 correctness bug**

证据：

- 当前 cache 把 Claim Spec 中每个设备的原始请求量乘以 count：
  `pkg/scheduler/cache/cache.go:1905-1921`、`1991-2005`。
- 文档也用“2 × 8Gi = 16Gi”定义逻辑用量：
  `docs/design/capacity-dra-support.md:143-154`。
- Kubernetes v1.36 中：
  - `requestPolicy` 可以把请求量向上调整；
  - 未显式请求某 dimension 时，实际默认消耗可能是整个 device capacity；
  - 最终值记录在 allocation result 的
    `results[*].consumedCapacity`。

影响：

- queue status 和 hard check 可能低于设备实际消耗；
- 同一 Claim 在不同可选设备上可能产生不同的实际 consumed capacity；
- partitionable / multi-allocation device 尤其容易出现差异。

建议：

- 二选一明确契约：
  1. **requested-capacity quota**：只限制用户在 Claim Spec 中声明的原始数量。实现较简单，但必须改名并明确它不是物理 usage；
  2. **consumed-capacity quota**：必须使用 Kubernetes 选中节点后的 allocation result，并处理 Reserve/rollback/session restore。不能只读 Claim Spec。
- 第一版建议选择 1，并把 requestPolicy、默认 consumption 和 partitionable device 明确列为不保证物理上限的场景。

### 4.11 Medium：Kubernetes v1.36 DRA Extended Resource 完全不计入 DRA quota

类型：**实现缺口 / 未声明边界**

证据：

- `buildTaskDRAInfo` 在 `pod.Spec.ResourceClaims` 为空时直接返回：
  `pkg/scheduler/cache/cache.go:1924-1927`。
- Kubernetes v1.36 的 `DRAExtendedResource` 已为 beta；native
  `DynamicResources` 会在 PreFilter/Filter 中为 Pod 的 extended resource
  构造或复用特殊 ResourceClaim。

影响：

- 使用 extended resource 兼容入口的 DRA workload 不会消耗
  `deviceclass/*` quota；
- 它可能仍消耗普通 extended resource queue quota，但这和 DeviceClass quota
  是两个不同配置键，不能替代设计承诺。

建议：

- 在支持矩阵中明确 DRA Extended Resource 不受 DeviceClass quota 管理；
- 长期应复用 native DynamicResources 生成的实际 Claim/Request mapping，避免 cache 自己再推导一次。

### 4.12 Medium：root queue “自动集群总量”没有实现，也很难按 DeviceClass 唯一计算

类型：**文档与实现不一致**

证据：

- 文档称 DRA cluster-wide total 可以来自实际 cluster state：
  `docs/design/capacity-dra-support.md:156-162`。
- capacity plugin 没有读取 `ResourceSlice` 或 `DeviceClass`；
- DRA total 只来自 Queue 的 `capability` / `deserved` / `guarantee`
  parser：`pkg/scheduler/plugins/capacity/capacity.go:142-192`。

设计上还存在一个更基本的问题：多个 DeviceClass selector 可以匹配同一物理设备，因此“按 DeviceClass 自动汇总 total”不一定可加，也不一定唯一。

建议：

- 当前版本明确为 **仅管理员显式配置 root DRA capability**；
- 不承诺从 ResourceSlice 自动计算 DeviceClass total；
- 如果未来需要自动 total，必须先定义 DeviceClass 重叠、node-local/global device、
  partitionable device 和 incomplete ResourceSlice generation 的处理规则。

### 4.13 Medium：Queue Status 与 enforcement 使用不同的启用条件和数据语义

类型：**实现一致性问题**

证据：

- Queue Status 在 session close 无条件从 TaskInfo 合并 DRA：
  `pkg/scheduler/framework/session.go:501-556`。
- status 合并按 Claim key 去重：
  `pkg/scheduler/framework/session_dra_queue_status.go:31-108`。
- capacity enforcement 受 plugin arguments 控制：
  `pkg/scheduler/plugins/capacity/capacity.go:412-430`。
- TaskInfo 的 capacity 摘要又受全局 Kubernetes feature gate 控制：
  `pkg/scheduler/cache/cache.go:1924-1932`。

影响：

- 可能出现 status 展示 DRA usage、但 capacity plugin 没有 enforce；
- 也可能 plugin 参数打开 consumable capacity，而全局 feature gate 关闭，导致 capacity 摘要为空并静默放行；
- status 展示的是 Claim Spec 请求量，不是 allocation result 的实际 consumption。

建议：

- 文档分别说明“观测”和“enforcement”的启用条件；
- plugin 参数不应允许制造与全局 feature gate 相矛盾的半启用状态，至少应在 session open 明确报错；
- Queue Status 的字段名称或文档应注明是 requested quota usage。

## 5. 尚未定义或未覆盖的边界

### 5.1 shared Claim 的计费域

当前设计只说“每个 Claim 一次”，但没有回答：

- 同一 Claim 被同一 queue 的多个 Job 引用：一次还是每个 Job 一次？
- 同一 Claim 被两个 leaf queue 引用：每个 leaf 各一次，还是只归属一个 queue？
- 共同祖先和 root 应计一次还是两次？
- Claim 删除后同名重建，旧 Task 与新 Task 如何区分？

建议以 Claim UID 为 identity，并明确 leaf、ancestor、root 各自的计费域。

### 5.2 heterogeneous gang 的最小 DRA 需求

没有 `TaskMinAvailable` 时，`GetMinDRAResources` 从带 DRA 的 Task 中按名字排序，取前
`MinAvailable` 个：`pkg/scheduler/api/job_info.go:1525-1552`。

如果一个 Job 同时有无 DRA Task 或不同 DRA 请求，真正能构成 gang 的 Task 组合不唯一。
当前算法既不一定是下界，也不一定对应 scheduler 实际先调度的组合。设计需要说明：

- 是否要求 DRA Job 必须提供 per-task-role minAvailable；
- 或 admission 使用保守上界；
- 或只在实际 Task allocation 阶段 enforce，不把不确定的组合用于 Job enqueue。

### 5.3 allocation 已存在的 Claim

ExactCount 的 Spec 是 immutable，device count 通常可以按 Spec 记账；但 capacity 的真实消耗可能只在
`status.allocation.devices.results[*].consumedCapacity` 中确定。设计需要说明：

- 未分配 Claim 用 request 预留多少；
- Reserve/PreBind 后是否 reconcile；
- 调度回滚如何撤销；
- scheduler restart 如何从 Claim Status 恢复。

### 5.4 DeviceClass 重叠

DeviceClass 是 selector，不是 driver 或独占资源池。两个 class 可以匹配同一设备。按请求中的 class
计逻辑 quota 是可行的，但不能把不同 class 的 quota 相加解释成物理总量。

### 5.5 Kubernetes v1.36 Workload ResourceClaims

v1.36 alpha `DRAWorkloadResourceClaims` 支持 native Workload/PodGroup 级 Claim，并允许同一
PodGroup scheduling cycle 复用 pending allocation。当前 Volcano TaskInfo 只从 Pod
`spec.resourceClaims` 生成 quota 摘要，也没有为 capacity accounting 定义 native PodGroup Claim
identity 和生命周期。

该能力可以延期，但需要列入 unsupported matrix，避免把“ResourceClaimTemplate 支持”误解为也支持
PodGroup 级 template。

### 5.6 admin access、taints、binding conditions、partitionable device

- admin access 是否消耗普通 queue quota；
- device taint/toleration 是否影响 admission 估算；
- binding condition 长时间 pending 时 inqueue reservation 如何保持；
- partitionable device 的 count 是 device、share 还是 allocation result 数；

这些都尚未定义。第一版可以延期，但应明确采用 fail-open、fail-closed 还是“不允许与 DRA quota 混用”。

## 6. 哪些限制是合理的

以下边界本身合理，不需要让 capacity plugin 复制 Kubernetes allocator：

- 不负责 node/device 物理放置；
- 不解决单设备 capacity fragmentation；
- DeviceClass quota 是 queue policy，不替代 ResourceSlice 可用性检查；
- 第一版只支持 `Exactly + ExactCount`；
- root total 由管理员显式配置。

问题不在于范围窄，而在于当前实现对超出范围的请求静默当成 0，以及文档仍宣称完整的
`capability/deserved/guarantee` 等价语义。

## 7. 建议的修订方向

### 7.1 第一阶段：收窄并修正 hard capability

建议把第一版契约明确为：

- 只支持 Pod 显式 ResourceClaim 和 Pod 级 ResourceClaimTemplate；
- 只支持 `Exactly + ExactCount`；
- count quota 为非负整数，0 表示禁止使用；
- capacity quota 明确定义为 **raw requested capacity quota**；
- 未配置的 capacity dimension 不限制；
- 只承诺 `capability`，暂不承诺 DRA `deserved` / `guarantee`；
- `FirstAvailable`、`All`、DRA Extended Resource、native Workload Claim 在配置了相关 DRA quota
  时明确拒绝或报告 unsupported，不静默跳过；
- root DRA capability 只接受管理员显式配置。

这一阶段同时修复：

1. direct Claim informer race；
2. shared Claim 的 Job min-resource 去重；
3. DRA inqueue session 重建；
4. hierarchy 按 Claim identity 重建；
5. Queue Status 和 enforcement 的语义标注。

### 7.2 第二阶段：再决定是否实现 DRA fair share

只有明确需要 DRA `deserved` / `guarantee` 时再实现，至少需要统一接入：

- queue share / order；
- reclaim 与 preemption；
- guarantee；
- hierarchical deserved/guarantee；
- allocated、request、inqueue、elastic；
- metrics 和 Queue Status。

不能只解析配置字段而不把 DRA usage 放进上述所有路径。

### 7.3 第三阶段：实际 allocation-aware capacity

如果需求是限制 Kubernetes 实际 `ConsumedCapacity`，应复用 native DynamicResources 的 allocation
result，而不是继续扩展 cache 中的 Spec 摘要：

- Filter 之后才知道具体 device/subrequest 和 adjusted capacity；
- Reserve/Unreserve 对应 pending allocation 生命周期；
- PreBind 后 Claim Status 成为持久事实；
- scheduler restart 从 allocated/pending Claim 恢复。

这会显著扩大实现范围，应与“raw requested quota”分成独立特性。

## 8. 测试建议

### 8.1 必须新增的单元测试

1. `FirstAvailable` 在 DRA hard quota 下不会被当成零请求；
2. `allocationMode: All` 在 DRA hard quota 下不会被当成零请求；
3. mixed Claim 中一个支持 request、一个 unsupported request 不会部分计费后放行；
4. count capability=0 拒绝正数请求；
5. 负数/小数 count capability 配置被拒绝；
6. count-only capability 允许带 capacity request 的 Claim；
7. 只配置某个 capacity dimension 时，未配置 dimension 不受限；
8. direct Claim informer NotFound 后 Task 留在 cache 并能 resync；
9. 两个 minAvailable Task 共享一个 Claim，Job enqueue 只计一次；
10. 同一 Claim 被不同 task role 引用时仍只计一次；
11. Inqueue DRA Job 跨两个 session 仍保留 reservation；
12. 已部分 allocated 的 gang 不在 allocated 和 inqueue 中重复计费；
13. parent-only DRA capability 能看见 child 的既有 allocation；
14. 两个 child queue 共享 Claim 时，parent 按定义的计费域去重；
15. session runtime 增量状态与下一 session 重建结果一致；
16. Queue Status、leaf enforcement、ancestor enforcement 对同一组 Task 给出一致 usage。

### 8.2 consumable capacity 测试

1. request count × raw requested capacity；
2. fully-qualified dimension 的 API/key 行为；
3. requestPolicy rounding，明确证明 status 是 requested 还是 consumed；
4. 未显式请求 capacity 时的默认 consumption；
5. 一个 Claim 多个 request、多个 DeviceClass；
6. partitionable/multi-allocation device 明确拒绝或按契约处理。

### 8.3 E2E 缺口

现有 E2E 覆盖 direct Claim、Pod 级 template、简单 shared Claim、count hard limit、capacity hard limit
和删除后恢复，基本 happy path 是有价值的；但还需要：

- shared Claim 使用 `replicas=2, minAvailable=2`，避免当前测试绕过 enqueue 去重；
- scheduler session/restart 后的 inqueue reservation；
- hierarchical parent-only quota；
- unsupported request 的明确失败事件；
- queue `deserved` / `guarantee` 只有在真正实现后再增加 borrow/reclaim E2E；
- Queue Status 检查应比较完整 DRA key 集合，而不只是
  `queueAllocatedContains` 的子集匹配：
  `test/e2e/dra/dra_quota_test.go:396-417`。

## 9. 建议修改后的设计结论

设计文档当前最需要的不是继续扩展支持列表，而是把契约改成可验证的窄闭环：

> Volcano capacity plugin 第一版为显式 Pod ResourceClaim 的
> `Exactly + ExactCount` 提供按 DeviceClass 的 queue hard capability。
> Consumable capacity 表示 Claim Spec 的原始请求量，不表示 Kubernetes 最终
> `ConsumedCapacity`。未支持的 DRA 请求不会被静默视为零。
> DRA `deserved`、`guarantee`、native Workload Claim、Extended Resource、
> `FirstAvailable`、`All` 和 partitionable device 暂不承诺。

先修复 hard capability、shared Claim 和 session/hierarchy 一致性，再讨论 fair share 与实际
consumed capacity，范围最小，也最不容易形成第二套不完整的 DRA allocator。

## 10. 上游参考

- [Kubernetes Dynamic Resource Allocation](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
- [Kubernetes v1.36 ResourceClaim API](https://kubernetes.io/docs/reference/kubernetes-api/resource/resource-claim-v1/)
- [Kubernetes v1.36 DRA updates](https://kubernetes.io/blog/2026/05/07/kubernetes-v1-36-dra-136-updates/)
- [Kubernetes v1.36.1 DynamicResources scheduler plugin](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go)
- [Kubernetes v1.36.1 DRA API types](https://github.com/kubernetes/kubernetes/blob/v1.36.1/staging/src/k8s.io/api/resource/v1/types.go)
