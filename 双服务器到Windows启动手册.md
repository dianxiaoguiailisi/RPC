# 双服务器到 Windows 启动手册

本文记录当前已经跑通的部署方式：

```text
119.45.130.240：Zookeeper + provider-1
175.27.163.96：provider-2
Windows 主机：consumer + JMeter 压测
```

完整调用链路：

```text
Windows JMeter/curl
-> Windows consumer(8881)
-> SSH 隧道 127.0.0.1:12181
-> 119 Zookeeper(2181)
-> 服务发现得到 provider 地址
-> Windows 直连 provider 的 9991 端口
-> provider 执行 RPC 服务并返回结果
```

## 1. 前置要求

两台服务器都需要：

```bash
java -version
mvn -v
```

建议使用 JDK 11。

Windows 也建议使用 JDK 11：

```powershell
$env:JAVA_HOME="E:\java\jdk\jdk11"
$env:MAVEN_HOME="D:\Java\apache-maven-3.8.6"
$env:Path="$env:JAVA_HOME\bin;$env:MAVEN_HOME\bin;$env:Path"

java -version
mvn -v
```

## 2. 启动 119 上的 Zookeeper

登录 119：

```bash
ssh ubuntu@119.45.130.240
```

启动 Docker 版 Zookeeper：

```bash
sudo docker rm -f wxy-zk

sudo docker run -d \
  --name wxy-zk \
  --restart=always \
  --network host \
  -e ZOO_CLIENT_PORT=2181 \
  -e ZOO_4LW_COMMANDS_WHITELIST=ruok,stat,conf,cons \
  zookeeper:3.7
```

检查：

```bash
sudo docker ps
ss -lntp | grep 2181
echo ruok | nc 127.0.0.1 2181
```

正常返回：

```text
imok
```

## 3. 启动 119 上的 provider-1

进入项目目录：

```bash
cd ~/wxy-rpc
```

确认 Netty 服务端监听所有网卡：

```bash
grep -n "serverBootstrap.bind(port)" rpc-server-spring-boot/src/main/java/com/wxy/rpc/server/transport/netty/NettyRpcServer.java
```

应该看到：

```text
serverBootstrap.bind(port).sync();
```

停止旧 provider：

```bash
ps -ef | grep ProviderApplication
kill -9 Maven进程PID Java进程PID
```

如果只剩下 `grep ProviderApplication`，说明旧进程已经停止。

重新编译：

```bash
mvn -pl provider -am clean install -DskipTests
```

启动 provider-1：

```bash
mvn -pl provider spring-boot:run \
  -Dspring-boot.run.main-class=com.wxy.rpc.provider.ProviderApplication \
  -Dspring-boot.run.arguments="--rpc.server.app-name=provider-1 --rpc.server.address=119.45.130.240 --rpc.server.registry-addr=127.0.0.1:2181 --rpc.server.port=9991"
```

另开一个 119 终端检查：

```bash
ss -lntp | grep 9991
```

正确结果应该是：

```text
*:9991
```

或者：

```text
[::]:9991
```

不能是：

```text
127.0.1.1:9991
```

## 4. 启动 175 上的 provider-2

登录 175：

```bash
ssh ubuntu@175.27.163.96
```

进入项目目录：

```bash
cd ~/wxy-rpc
```

确认 Netty 服务端监听所有网卡：

```bash
grep -n "serverBootstrap.bind(port)" rpc-server-spring-boot/src/main/java/com/wxy/rpc/server/transport/netty/NettyRpcServer.java
```

停止旧 provider：

```bash
ps -ef | grep ProviderApplication
kill -9 Maven进程PID Java进程PID
```

重新编译：

```bash
mvn -pl provider -am clean install -DskipTests
```

启动 provider-2：

```bash
mvn -pl provider spring-boot:run \
  -Dspring-boot.run.main-class=com.wxy.rpc.provider.ProviderApplication \
  -Dspring-boot.run.arguments="--rpc.server.app-name=provider-2 --rpc.server.address=175.27.163.96 --rpc.server.registry-addr=119.45.130.240:2181 --rpc.server.port=9991"
```

另开一个 175 终端检查：

```bash
ss -lntp | grep 9991
```

正确结果同样应该是：

```text
*:9991
```

或者：

```text
[::]:9991
```

## 5. 云服务器安全组

两台服务器都要放行 RPC 端口：

```text
TCP 9991
来源：Windows 当前公网 IP/32
```

临时测试可以先放：

```text
TCP 9991
来源：0.0.0.0/0
```

测通后建议收紧。

如果 Windows 不通过 SSH 隧道直连 Zookeeper，119 还需要放行：

```text
TCP 2181
来源：Windows 当前公网 IP/32
```

当前推荐走 SSH 隧道，所以 2181 不需要对公网开放。

## 6. Windows 开 SSH 隧道

Windows 新开一个 PowerShell，执行后不要关闭：

```powershell
ssh -N -L 12181:127.0.0.1:2181 ubuntu@119.45.130.240
```

另开一个 PowerShell 检查：

```powershell
Test-NetConnection 127.0.0.1 -Port 12181
```

正常结果：

```text
TcpTestSucceeded : True
```

## 7. Windows 检查 provider 端口

在 Windows PowerShell 执行：

```powershell
Test-NetConnection 119.45.130.240 -Port 9991
Test-NetConnection 175.27.163.96 -Port 9991
```

两台都必须是：

```text
TcpTestSucceeded : True
```

如果是 `False`，先检查：

```text
1. provider 是否启动
2. ss -lntp | grep 9991 是否为 *:9991 或 [::]:9991
3. 云服务器安全组是否放行 9991
```

## 8. Windows 启动 consumer

进入项目目录：

```powershell
cd C:\Users\10567\Desktop\RPC
```

切换 JDK 11：

```powershell
$env:JAVA_HOME="E:\java\jdk\jdk11"
$env:MAVEN_HOME="D:\Java\apache-maven-3.8.6"
$env:Path="$env:JAVA_HOME\bin;$env:MAVEN_HOME\bin;$env:Path"
```

如果是第一次运行或更新过代码，先安装依赖：

```powershell
mvn -pl consumer -am clean install -DskipTests
```

启动 consumer：

```powershell
mvn -pl consumer spring-boot:run `
  "-Dspring-boot.run.main-class=com.wxy.rpc.consumer.ConsumerApplication" `
  "-Dspring-boot.run.arguments=--rpc.client.registry-addr=127.0.0.1:12181 --rpc.client.serialization=HESSIAN --rpc.client.timeout=15000"
```

启动成功后会看到：

```text
Tomcat started on port(s): 8881
Started ConsumerApplication
```

## 9. 功能测试

Windows 新开 PowerShell：

```powershell
curl.exe http://127.0.0.1:8881/hello/zhangsan
```

预期返回：

```text
Hello, zhangsan
```

串行调用测试：

```powershell
curl.exe http://127.0.0.1:8881/hello/test/1000
```

注意：`/hello/test/1000` 是单线程串行调用 1000 次 RPC，不代表并发 TPS。

## 10. JMeter 压测

启动 JMeter：

```powershell
D:\Java\apache-jmeter-5.6.3\bin\jmeter.bat
```

创建线程组：

```text
Test Plan
-> Add
-> Threads (Users)
-> Thread Group
```

线程组第一轮配置：

```text
Number of Threads: 50
Ramp-up period: 10
Loop Count: 100
```

添加 HTTP Request：

```text
Thread Group
-> Add
-> Sampler
-> HTTP Request
```

HTTP Request 配置：

```text
Protocol: http
Server Name or IP: 127.0.0.1
Port Number: 8881
Method: GET
Path: /hello/zhangsan
```

添加统计报表：

```text
Thread Group
-> Add
-> Listener
-> Aggregate Report

Thread Group
-> Add
-> Listener
-> Summary Report
```

正式压测时不要添加或启用 `View Results Tree`，它会保存每个请求结果，影响性能。

建议三轮压测：

```text
第一轮：50 线程，Loop 100，总请求 5000
第二轮：100 线程，Loop 100，总请求 10000
第三轮：200 线程，Loop 100，总请求 20000
```

记录以下指标：

```text
# Samples
Average
90% Line
95% Line
99% Line
Error %
Throughput
```

## 11. 常见问题

### 11.1 Windows 调用 provider 失败

现象：

```text
Test-NetConnection providerIP -Port 9991
TcpTestSucceeded : False
```

排查：

```bash
ss -lntp | grep 9991
```

如果监听是：

```text
127.0.1.1:9991
```

说明 provider 没有监听公网网卡，需要确认 `NettyRpcServer` 中是：

```java
serverBootstrap.bind(port).sync();
```

然后重新编译并重启 provider。

### 11.2 consumer 连不上 Zookeeper

现象：

```text
ConnectException: 127.0.0.1:12181 连接被拒绝
```

说明 SSH 隧道没有开。重新执行：

```powershell
ssh -N -L 12181:127.0.0.1:2181 ubuntu@119.45.130.240
```

### 11.3 JDK 21 下 CGLIB 报错

现象：

```text
InaccessibleObjectException
module java.base does not "opens java.lang"
```

原因是项目使用 CGLIB 3.1，JDK 21 模块限制更严格。建议统一使用 JDK 11。

### 11.4 Zookeeper 四字命令无响应

Docker 版 Zookeeper 要开启白名单：

```bash
-e ZOO_4LW_COMMANDS_WHITELIST=ruok,stat,conf,cons
```

然后测试：

```bash
echo ruok | nc 127.0.0.1 2181
```

返回：

```text
imok
```

## 12. 启动顺序总结

```text
1. 119 启动 Zookeeper
2. 119 启动 provider-1
3. 175 启动 provider-2
4. Windows 开 SSH 隧道 127.0.0.1:12181 -> 119:2181
5. Windows 启动 consumer
6. Windows curl 功能测试
7. Windows JMeter 压测
```

