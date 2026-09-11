<p align="center">
<img src="https://raw.githubusercontent.com/panjf2000/logos/master/ants/logo.png" />
<b>Rust 语言的工作线程池</b>
<br/><br/>
<a title="Build Status" target="_blank" href="https://github.com/panjf2000/ants/actions?query=workflow%3ATests"><img src="https://img.shields.io/github/actions/workflow/status/panjf2000/ants/test.yml?branch=master&style=flat-square&logo=github-actions" /></a>
<a title="Codecov" target="_blank" href="https://codecov.io/gh/panjf2000/ants"><img src="https://img.shields.io/codecov/c/github/panjf2000/ants?style=flat-square&logo=codecov" /></a>
<a title="Release" target="_blank" href="https://github.com/panjf2000/ants/releases"><img src="https://img.shields.io/github/v/release/panjf2000/ants.svg?color=161823&style=flat-square&logo=smartthings" /></a>
<a title="Tag" target="_blank" href="https://github.com/panjf2000/ants/tags"><img src="https://img.shields.io/github/v/tag/panjf2000/ants?color=%23ff8936&logo=fitbit&style=flat-square" /></a>
<br/>
<a title="Minimum Rust Version" target="_blank" href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/rust-%3E%3D1.75-30dff3?style=flat-square&logo=rust" /></a>
<a title="Doc for ants" target="_blank" href="https://docs.rs/ants"><img src="https://img.shields.io/badge/docs.rs-doc-007d9c?style=flat-square&logo=read-the-docs" /></a>
</p>

[英文](README.md) | 中文

## 📖 简介

`ants` 是一个高性能的工作线程池，实现了对大规模工作线程的调度管理、线程复用，允许使用者在开发并发程序的时候限制线程数量，复用资源，达到更高效执行任务的效果。

## 🚀 功能：

- 自动调度海量的工作线程，复用工作线程
- 定期清理过期的工作线程，进一步节省资源
- 提供了大量实用的接口：任务提交、获取运行中的工作线程数量、动态调整 Pool 大小、释放 Pool、重启 Pool 等
- 优雅处理 panic，防止程序崩溃
- 资源复用，极大节省内存使用量；在大规模批量并发任务场景下甚至可能比 Rust 语言的无限制线程并发具有***更高的性能***
- 非阻塞机制
- 预分配内存 (环形队列，可选)

## 💡 `ants` 是如何运行的

### 流程图

<p align="center">
<img width="845" alt="ants-flowchart-cn" src="https://user-images.githubusercontent.com/7496278/66396519-7ed66e00-ea0c-11e9-9c1a-5ca54bbd61eb.png">
</p>

### 动态图

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-1.png)

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-2.png)

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-3.png)

![](https://raw.githubusercontent.com/panjf2000/illustrations/master/go/ants-pool-4.png)

## 🧰 安装

在 `Cargo.toml` 中加入 `ants`：

```toml
[dependencies]
ants = "2"
```

或者交给 Cargo 处理：

```shell
cargo add ants
```

`ants` 需要 stable Rust 1.75 或更新的版本，除了用于渲染默认日志时间戳的 `chrono` 之外没有其他依赖。

## 🛠 使用
基本的使用请查看[示例](https://docs.rs/ants).

### Pool 配置

通过在调用 `Pool::new`/`PoolWithFunc::new`/`PoolWithFuncGeneric::new` 之时传入各种 option function，可以设置 `ants::Options` 中各个配置项的值，然后用它来定制化工作线程池。

Rust 没有可变参数，所以原版的 `options ...Option` 在这里是一个 `ants::Opt` 切片；该类型命名为 `Opt` 是为了不遮蔽 `std::option::Option`。

### 自定义 pool 容量
`ants` 支持实例化使用者自己的一个 Pool，指定具体的 pool 容量；通过调用 `Pool::new` 方法可以实例化一个新的带有指定容量的 `Pool`，如下：

``` rust
let p = ants::Pool::new(10000, &[]).unwrap();
```

### 任务提交

提交任务通过调用 `ants::submit` 方法：
```rust
ants::submit(ants::task(|| {})).unwrap();
```

### 动态调整线程池容量
需要动态调整 pool 容量可以通过调用 `Pool::tune`：

``` rust
pool.tune(1000); // Tune its capacity to 1000
pool.tune(100000); // Tune its capacity to 100000
```

该方法是线程安全的。

### 预先分配工作线程队列内存

`ants` 支持预先为 pool 分配容量的内存， 这个功能可以在某些特定的场景下提高线程池的性能。比如， 有一个场景需要一个超大容量的池，而且每个任务都是耗时任务，这种情况下，预先分配工作线程队列内存将会减少不必要的内存重新分配。

```rust
// 提前分配的 pool 容量的内存空间
let p = ants::Pool::new(100000, &[ants::with_pre_alloc(true)]).unwrap();
```

### 释放 Pool

```rust
pool.release();
```

或者

```rust
pool.release_timeout(ants::Duration::SECOND * 3).unwrap();
```

### 重启 Pool

```rust
// 只要调用 reboot() 方法，就可以重新激活一个之前已经被销毁掉的池，并且投入使用。
pool.reboot();
```

## ⚙️ 关于任务执行顺序

`ants` 并不保证提交的任务被执行的顺序，执行的顺序也不是和提交的顺序保持一致，因为在 `ants` 是并发地处理所有提交的任务，提交的任务会被分派到正在并发运行的 workers 上去，因此那些任务将会被并发且无序地被执行。

## 👏 贡献者

请在提 PR 之前仔细阅读 [Contributing Guidelines](CONTRIBUTING.md)，感谢那些为 `ants` 贡献过代码的开发者！

<a href="https://github.com/panjf2000/ants/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=panjf2000/ants" />
</a>

## 📄 证书

`ants` 的源码允许用户在遵循 [MIT 开源证书](/LICENSE) 规则的前提下使用。

## 📚 相关文章

-  [Goroutine 并发调度模型深度解析之手撸一个高性能 goroutine 池](https://taohuawu.club/high-performance-implementation-of-goroutine-pool)
-  [Visually Understanding Worker Pool](https://medium.com/coinmonks/visually-understanding-worker-pool-48a83b7fc1f5)
-  [The Case For A Go Worker Pool](https://brandur.org/go-worker-pool)
-  [Go Concurrency - GoRoutines, Worker Pools and Throttling Made Simple](https://twin.sh/articles/39/go-concurrency-goroutines-worker-pools-and-throttling-made-simple)

## 🖥 用户案例

### 商业公司和开源组织

以下公司/组织在生产环境上使用了 `ants`。

<table>
  <tbody>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.tencent.com/">
          <img src="https://res.strikefreedom.top/static_res/logos/tencent_logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.bytedance.com/zh/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/ByteDance_Logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://tieba.baidu.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/baidu-tieba-logo.png" width="300" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://weibo.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/weibo-logo.png" width="300" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.tencentmusic.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/tencent-music-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.futuhk.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/futu-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.shopify.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/shopify-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://weixin.qq.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/wechat-logo.png" width="250" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.baidu.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/baidu-mobile-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.360.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/360-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.huaweicloud.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/huawei-cloud-logo-cn.svg" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://matrixorigin.cn/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/matrix-origin-logo-cn.svg" width="250" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://adguard-dns.io/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/adguard-dns-black.svg" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://bk.tencent.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/tencent-bk-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://cn.aliyun.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/aliyun-cn-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.zuoyebang.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/zuoyebang-logo.jpeg" width="300" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.antgroup.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/ant-group-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://zilliz.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/zilliz-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://amap.com/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/amap-logo.png" width="250" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a href="https://www.apache.org/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/asf-estd-1999-logo.jpg" width="250" />
        </a>
      </td>
    </tr>
    <tr>
      <td align="center" valign="middle">
        <a href="https://www.coze.cn/" target="_blank">
          <img src="https://res.strikefreedom.top/static_res/logos/coze-logo-cn.png" width="250" />
        </a>
      </td>
    </tr>
  </tbody>
</table>
如果你也正在生产环境上使用 `ants`，欢迎提 PR 来丰富这份列表。

### 开源软件

这些开源项目借助 `ants` 进行并发编程。

- [gnet](https://github.com/panjf2000/gnet):  gnet 是一个高性能、轻量级、非阻塞的事件驱动 Go 网络框架。
- [milvus](https://github.com/milvus-io/milvus): 一个高度灵活、可靠且速度极快的云原生开源向量数据库。
- [nps](https://github.com/ehang-io/nps): 一款轻量级、高性能、功能强大的内网穿透代理服务器。
- [TDengine](https://github.com/taosdata/TDengine): TDengine 是一款开源、高性能、云原生的时序数据库 (Time-Series Database, TSDB)。TDengine 能被广泛运用于物联网、工业互联网、车联网、IT 运维、金融等领域。
- [siyuan](https://github.com/siyuan-note/siyuan): 思源笔记是一款本地优先的个人知识管理系统，支持完全离线使用，同时也支持端到端加密同步。
- [BillionMail](https://github.com/aaPanel/BillionMail): BillionMail 是一个未来的开源邮件服务器和电子邮件营销平台，旨在帮助企业和个人轻松管理他们的电子邮件营销活动。
- [WeKnora](https://github.com/Tencent/WeKnora): 一款基于大语言模型（LLM）的文档理解与语义检索框架，专为结构复杂、内容异构的文档场景而打造。
- [coze-loop](https://github.com/coze-dev/coze-loop): Coze Loop 是一个面向开发者，专注于 AI Agent 开发与运维的平台级解决方案。
- [osmedeus](https://github.com/j3ssie/osmedeus): A Workflow Engine for Offensive Security.
- [jitsu](https://github.com/jitsucom/jitsu/tree/master): An open-source Segment alternative. Fully-scriptable data ingestion engine for modern data teams. Set-up a real-time data pipeline in minutes, not days.
- [triangula](https://github.com/RH12503/triangula): Generate high-quality triangulated and polygonal art from images.
- [teler](https://github.com/kitabisa/teler): Real-time HTTP Intrusion Detection.
- [bsc](https://github.com/binance-chain/bsc): A Binance Smart Chain client based on the go-ethereum fork.
- [jaeles](https://github.com/jaeles-project/jaeles): The Swiss Army knife for automated Web Application Testing.
- [devlake](https://github.com/apache/incubator-devlake): The open-source dev data platform & dashboard for your DevOps tools.
- [matrixone](https://github.com/matrixorigin/matrixone): MatrixOne 是一款面向未来的超融合异构云原生数据库，通过超融合数据引擎支持事务/分析/流处理等混合工作负载，通过异构云原生架构支持跨机房协同/多地协同/云边协同。简化开发运维，消简数据碎片，打破数据的系统、位置和创新边界。
- [bk-bcs](https://github.com/TencentBlueKing/bk-bcs): 蓝鲸容器管理平台（Blueking Container Service）定位于打造云原生技术和业务实际应用场景之间的桥梁；聚焦于复杂应用场景的容器化部署技术方案的研发、整合和产品化；致力于为游戏等复杂应用提供一站式、低门槛的容器编排和服务治理服务。
- [trueblocks-core](https://github.com/TrueBlocks/trueblocks-core): TrueBlocks improves access to blockchain data for any EVM-compatible chain (particularly Ethereum mainnet) while remaining entirely local.
- [openGemini](https://github.com/openGemini/openGemini): openGemini 是华为云开源的一款云原生分布式时序数据库，可广泛应用于物联网、车联网、运维监控、工业互联网等业务场景，具备卓越的读写性能和高效的数据分析能力，采用类SQL查询语言，无第三方软件依赖、安装简单、部署灵活、运维便捷。
- [AdGuardDNS](https://github.com/AdguardTeam/AdGuardDNS): AdGuard DNS is an alternative solution for tracker blocking, privacy protection, and parental control.
- [WatchAD2.0](https://github.com/Qihoo360/WatchAD2.0): WatchAD2.0 是 360 信息安全中心开发的一款针对域安全的日志分析与监控系统，它可以收集所有域控上的事件日志、网络流量，通过特征匹配、协议分析、历史行为、敏感操作和蜜罐账户等方式来检测各种已知与未知威胁，功能覆盖了大部分目前的常见内网域渗透手法。
- [vanus](https://github.com/vanus-labs/vanus): Vanus is a Serverless, event streaming system with processing capabilities. It easily connects SaaS, Cloud Services, and Databases to help users build next-gen Event-driven Applications.
- [trpc-go](https://github.com/trpc-group/trpc-go): 一个 Go 实现的可插拔的高性能 RPC 框架。
- [motan-go](https://github.com/weibocom/motan-go): Motan 是一套高性能、易于使用的分布式远程服务调用 (RPC) 框架。motan-go 是 motan 的 Go 语言实现。

#### 所有案例:

- [Repositories that depend on ants/v2](https://github.com/panjf2000/ants/network/dependents?package_id=UGFja2FnZS0yMjY2ODgxMjg2)

- [Repositories that depend on ants/v1](https://github.com/panjf2000/ants/network/dependents?package_id=UGFja2FnZS0yMjY0ODMzNjEw)

如果你的项目也在使用 `ants`，欢迎给我提 Pull Request 来更新这份用户案例列表。

## 🔋 JetBrains 开源证书支持

`ants` 项目一直以来都是在 JetBrains 公司旗下的 GoLand 集成开发环境中进行开发，基于 **free JetBrains Open Source license(s)** 正版免费授权，在此表达我的谢意。

<a href="https://www.jetbrains.com/?from=ants" target="_blank"><img src="https://resources.jetbrains.com/storage/products/company/brand/logos/jetbrains.svg" alt="JetBrains logo."></a>

## ☕️ 打赏

> 当您通过以下方式进行捐赠时，请务必留下姓名、GitHub 账号或其他社交媒体账号，以便我将其添加到捐赠者名单中，以表谢意。

<table>
  <tbody>
    <tr>
      <td align="center" valign="middle">
        <a target="_blank" href="https://buymeacoffee.com/panjf2000">
          <img src="https://res.strikefreedom.top/static_res/logos/bmc_qr.png" width="250" alt="Buy me coffee" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a target="_blank" href="https://www.patreon.com/panjf2000">
          <img src="https://res.strikefreedom.top/static_res/logos/patreon_logo.png" width="250" alt="Patreon" />
        </a>
      </td>
      <td align="center" valign="middle">
        <a target="_blank" href="https://opencollective.com/panjf2000">
          <img src="https://res.strikefreedom.top/static_res/logos/open-collective-logo.png" width="250" alt="OpenCollective" />
        </a>
      </td>
    </tr>
  </tbody>
</table>

## 🔋 赞助商

[![DigitalOcean Referral Badge](https://web-platforms.sfo2.cdn.digitaloceanspaces.com/WWW/Badge%203.svg)](https://www.digitalocean.com/?refcode=5d8774f42124&utm_campaign=Referral_Invite&utm_medium=Referral_Program&utm_source=badge)
