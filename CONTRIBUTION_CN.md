# 贡献指南

| 更新时间   | 更新人                                         |
| ---------- | ---------------------------------------------- |
| 2022年10月 | @[xiangyu5632](https://github.com/xiangyu5632) |
| 2023年4月  | @[shilinlee](https://github.com/shilinlee)     |
| 2023年4月  | @[xmh1011](https://github.com/xmh1011)         |
| 2023年5月  | @[1156230954](https://github.com/1156230954)   |
| 2023年12月 | @[xiangyu5632](https://github.com/xiangyu5632) |

欢迎来到openGemini！

## 1. 源码目录结构

```
openGemini
├── app
├── benchmarks
├── build
├── config
├── coordinator
├── docker
├── docs
├── engine
├── images
├── lib
├── open_src
├── python
├── scripts
├── services
└── tests
```

| 目录        | 说明                                                         |
| ----------- | ------------------------------------------------------------ |
| app         | 包括ts-meta, ts-sql, ts-store, ts-monitor, ts-server, ts-cli等组件的启动和通信模块代码 |
| build       | openGemin源码编译后，二进制文件存放目录                      |
| config      | openGemini的配置文件存放目录                                 |
| coordinator | 分布式系统的协调层，主要负责将读写请求分发到不同ts-store节点，还包含DDL命令执行时与ts-meta的元数据交互 |
| docker      | 存放Docker部署相关的文件，比如Dockerfile，启动脚本等         |
| engine      | 存储引擎实现                                                 |
| lib         | 各种通用工具和支持函数的实现                                 |
| open_src    | 依赖的第三方开源组件代码（该目录后续会考虑删除）             |
| python      | 基于AI的时序数据分析平台实现，支持时序数据异常检测           |
| scripts     | 包含openGemini的自动部署脚本、单元测试脚本等                 |
| services    | openGemini的后台服务，比如连续查询将采样(Continue Query), 多级将采样(Downsample)等 |
| tests       | 包含openGemini的全部功能测试用例                             |

## 2. 行为守则

请务必阅读并遵守我们的[行为准则](./CODE_OF_CONDUCT.md)。

## 3. 提交Issue/处理issue任务

- 提交Issue
  如果您准备向社区上报Bug或者提交需求，或者为openGemini社区贡献自己的意见或建议，请在openGemini社区对应的仓库上提交Issue。

- 参与Issue内的讨论
  每个Issue下面可能已经有参与者们的交流和讨论，如果您感兴趣，也可以在评论框中发表自己的意见。

- 找到愿意处理的Issue
  如果您愿意处理其中的一个issue，可以将它分配给自己。只需要在评论框内输入 /assign或 /assign @yourself，机器人就会将问题分配给您，您的名字将显示在负责人列表里。

  > **@yourself** 替换为您的 GitHub 用户名，比如 /assign @bob

## 4. 贡献源码

### 4.1 提交拉取请求详细步骤

1. 在提交拉取请求之前，请先在 [Github](https://github.com/openGemini/openGemini/pulls) 中搜索关闭或开启的相关PR，以避免重复工作。

2. 确保问题描述了您正在修复的问题，或记录了您要添加的功能的设计。提前讨论设计有助于确保我们准备好接受您的工作。

3. 签署openGemini [DCO](https://developercertificate.org)（Developer Certificate of Origin，开发者原创声明），并遵守原创契约。每次提交PR时，都需使用邮箱进行签署 ，因为我们不能接受没有签名DCO的代码。

   > git commit 命令中增加-s参数，即可自动签名。比如：git commit -s -m "fix: xxxxxxxx"

4. [Fork](https://github.com/openGemini/openGemini/fork) openGemini/openGemini 仓库

   您需要了解如何在GitHub下载代码，通过PR合入代码等。openGemini使用GitHub代码托管平台，想了解具体的指导，请参考[GitHub Workflow Guide](https://docs.github.com/cn)。

5. Clone您的仓库，在您的仓库中，在新的git分支中更改：

   ```
   git checkout -b my-fix-branch main
   ```

6. 添加你的**代码**和**测试用例**

7. 使用git工具完成您的commit。

    ```
    // 添加文件到暂存区
    git add .
    
    // 提交暂存区到本地仓库，增加-s参数，自动签名
    git commit -s -m "<your commit message>"
    ```
   其中，`<your commit message>`是您的提交信息，需要遵循以下命名规范：
   - feat: feature的缩写, 新的功能或特性
   - fix: bug的修复
   - docs: 文档修改
   - style: 格式修改. 比如改变缩进, 空格, 删除多余的空行, 补上漏掉的分号. 总之, 就是不影响代码含义和功能的修改
   - refactor: 代码重构. 一些不算修复bug也没有加入新功能的代码修改
   - perf: performance的缩写, 提升代码性能
   - test: 测试文件的修改
   - chore: 其他的小改动. 一般为仅仅一两行的改动, 或者连续几次提交的小改动属于这种
   
   更多详细信息，您可以参考[约定式提交](https://www.conventionalcommits.org/zh-hans/v1.0.0/)。

8. 将您的分支推送到Github

   `git push origin my-fix-branch`

9. 打开PR开始合并请求
   当你提交一个PR的时候，就意味您已经开始给社区贡献代码了。请参考 openGemini社区PR提交指导。
   为了使您的提交更容易被接受，您需要：

   - 填写完善的提交信息，并且签署DCO。
   - 如果一次提交的代码量较大，建议将大型的内容分解成一系列逻辑上较小的内容，分别进行提交会更便于检视者理解您的想法

   注意：如果您的PR请求没有引起足够的关注，可以发送邮件到[community.ts@opengemini.org](mailto:community.ts@opengemini.org)求助。 s

### 4.2 编译源码

#### 4.2.1 支持平台

我们支持以下平台:

- Linux x86/arm（64bit）
- Darwin x86/arm（64bit）
- Windows x86（64bit）

#### 4.2.2 编译环境信息

[GO](https://golang.org/dl/) version v1.19+

[Python](https://www.python.org/downloads/) version v3.7+

[Git](https://git-scm.com/downloads)

#### 4.2.3 GO环境变量设置

打开 `~/.profile`配置文件，在文件末尾添加如下配置：

```bash
# 设置GOPATH(可自定义目录)
export GOPATH=$HOME/gocodez
export GOPROXY=https://goproxy.cn,direct
export GO111MODULE=on
export GONOSUMDB=*
export GOSUMDB=off
```

#### 4.2.4 下载源码编译

```bash
cd $GOPATH
mkdir -p {pkg,bin,src}
cd src
git clone git@github.com:<username>/openGemini.git
cd openGemini
python3 build.py --clean
```

编译成功后，二进制保存在`build`目录中。

### 4.3 启动服务

#### 4.3.1 启动单机版

- 默认参数启动

  ```bash
  ./build/ts-server
  ```

- 带配置文件启动

  ```
  ./build/ts-server run -config=config/openGemini.singlenode.conf  
  ```

- 使用脚本启动

  ```bash
  bash scripts/install.sh
  ```

#### 4.3.2 启动伪集群版

```bash
bash scripts/install_cluster.sh
```

> maxOS用户可能第一次运行需要输入admin的密码，参考 https://superuser.com/questions/458875/how-do-you-get-loopback-addresses-other-than-127-0-0-1-to-work-on-os-x , 临时放开 127.0.0.2和127.0.0.3

### 4.4 CI和静态分析工具

#### 4.4.1 CI

所有的pull requests都会运行CI。 社区贡献者应该查看PR checks的结果，来检查是否符合代码合入的最低门槛。 如果有任何问题请解决，以确保团队成员及时进行审核。

openGemini 项目在内部也有很多检查流程。 这可能需要一些时间，并且对社区贡献者来说并不真正可见。我们会定期将问题以及修复代码同步到社区。

#### 4.4.2 Static Analysis

该项目使用以下静态分析工具。 运行这些工具中的任何一个失败都会导致构建失败。 通常，必须调整代码以满足这些工具的要求，但也有例外。

- [go vet](https://golang.org/cmd/vet/) 用于分析Go 代码中的常见错误和潜在bug。它可以检查代码中可能存在的各种问题，例如： 未使用的变量、函数或包以及可疑的函数调用等

  通过在本项目根目录执行以下命令，即可：

  ```bash
  make go-vet-check
  ```

- [goimports-reviser](https://github.com/incu6us/goimports-reviser) import分组排序和代码格式化。

  通过在本项目根目录执行以下命令，即可：

  ```
  make style-check
  ```
  
- [go mod tidy](https://tip.golang.org/cmd/go/#hdr-Add_missing_and_remove_unused_modules) 下载并增加项目依赖的第三方开源组件名称和版本到go.mod文件，解决项目丢失依赖问题，同时也会去掉go.mod文件中项目不需要的依赖。

- [staticcheck](https://staticcheck.io/docs/) 检查以下内容：未使用的代码、可以简化的代码、不正确的代码、不安全的代码以及将出现性能问题的代码。

  通过在本项目根目录执行以下命令，即可：

  > 注意：由于static-check本身的问题，所以本地go版本在1.19及以上，才能执行。

  ```bash
  make static-check
  ```

## 5. 参与社区其他贡献

### 5.1 贡献生态工具

如果你发现其他第三方软件系统、工具缺失了对openGemini的支持，或者openGemini缺失了对南向操作系统、CPU架构、存储系统的支持，可以帮openGemini把这个支持补上。与此同时，社区也会在ISSUE中发布生态工具的开发任务，您也可以主动接下任务。贡献生态工具的过程是帮助openGemini繁荣生态的过程，让openGemini成为一个具有广泛技术生态的开源时序数据库系统。

社区流程：

- 在openGemini仓库下提交issue（custom），说明具体需求
- 提交PR并关联该issue
- 通过社区[邮件列表](https://groups.google.com/g/openGemini)/社区交流群等方式通知社区，并在社区进行一次分享
- 合入

> 如果是在其他第三方软件系统或者工具上实现了对openGemini的支持，可直接通过社区[邮件列表](https://groups.google.com/g/openGemini)/社区交流群等方式通知社区

### 5.2 贡献自己的开源项目

如果您想将自己原创的基于openGemini开发的应用或解决方案贡献到openGemini社区，可以在

直接在https://github.com/openGemini中建立原创项目，将项目“托管”到openGemini社区。

社区流程：

- 通过社区[邮件列表](https://groups.google.com/g/openGemini)/社区交流群等方式联系社区，说明项目基本情况，申请加入社区（无固定模板）
- 在社区例会分享该项目
- 等待社区审核通过后，在社区新建代码仓库，通知合入，并开通相应权限

### 5.3 检视代码

openGemini是一个开放的社区，我们希望所有参与社区的人都能成为活跃的代码检视者。当成为SIG组的committer或maintainer角色时，便拥有审核代码的责任与权利。
强烈建议本着[行为准则](./CODE_OF_CONDUCT.md)，相互尊重和促进协作，希望能够促进新的贡献者积极参与，而不会使贡献者一开始就被细微的错误淹没，所以检视的时候，可以重点关注包括：
 1.贡献背后的想法是否合理
 2.贡献的架构是否正确
 3.贡献是否完善

### 5.4 测试

为了成功发行一个社区版本，需要完成多种测试活动。不同的测试活动，测试代码的位置也有所不同，成功运行测试所需的环境的细节也会有差异，有关的信息可以参考：[社区开发者测试贡献指南]()。

### 5.5 参与非代码类贡献

如果您的兴趣不在编写代码方面，可以在[ 非代码贡献指南 ]()中找到感兴趣的工作。

## 6. 和社区一起成长

### 6.1 社区角色说明

社区不同角色对应不同的责任与权利，每种角色都是社区不可或缺的一部分，您可以通过积极贡献不断积累经验和影响力，并获得角色上的成长。更详细角色说明与责任权利描述请查看 [角色说明]()。

### 6.2 技术委员会

openGemini技术委员会（Technical Committee，简称TC）是openGemini社区的技术决策机构，负责社区技术决策和技术资源的协调。



