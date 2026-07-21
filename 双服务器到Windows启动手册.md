
## java、Mvn环境

如果服务器默认不是 JDK 11，先切换Windows PowerShell：
```powershell
$env:JAVA_HOME="E:\java\jdk\jdk11"
$env:MAVEN_HOME="D:\Java\apache-maven-3.8.6"
$env:Path="$env:JAVA_HOME\bin;$env:MAVEN_HOME\bin;$env:Path"
```
## 启动 两个服务器
> 119服务器
1. 启动Zoopkeeper
   - 登录 119：`ssh ubuntu@119.45.130.240`
   - 启动 Docker 版 Zookeeper：`sudo docker start wxy-zk`
2. 启动 119 的 provider-1
   - 下载代码：`git pull origin main`
   - 编译：`mvn -pl provider -am clean install -DskipTests`
   - 启动 provider-1：
     ```
     mvn -pl provider spring-boot:run \
       -Dspring-boot.run.main-class=com.wxy.rpc.provider.ProviderApplication \
       -Dspring-boot.run.arguments="--rpc.server.app-name=provider-1 --rpc.server.address=119.45.130.240 --rpc.server.registry-addr=127.0.0.1:2181 --rpc.server.port=9991 --logging.level.com.wxy.rpc=warn"
     ```
> 175服务器
1. 启动 175 的 provider-2
   - 登录 175：`ssh ubuntu@175.27.163.96`
   - 编译：`mvn -pl provider -am clean install -DskipTests`
   - 启动 provider-2：
     ```
     mvn -pl provider spring-boot:run \
       -Dspring-boot.run.main-class=com.wxy.rpc.provider.ProviderApplication \
       -Dspring-boot.run.arguments="--rpc.server.app-name=provider-2 --rpc.server.address=175.27.163.96 --rpc.server.registry-addr=119.45.130.240:2181 --rpc.server.port=9991 --logging.level.com.wxy.rpc=warn"
     ```
## Windows
1.  开 SSH 隧道`ssh -N -L 12181:127.0.0.1:2181 ubuntu@119.45.130.240`
   - 再开一个 PowerShell 检查：`Test-NetConnection 127.0.0.1 -Port 12181`；正常结果：`TcpTestSucceeded : True`
2. Windows 检查 provider 端口,两台都必须是：TcpTestSucceeded : True

   ```powershell
   Test-NetConnection 119.45.130.240 -Port 9991
   Test-NetConnection 175.27.163.96 -Port 9991
   ```

3. Windows 启动 consumer

   - 切换 JDK 11 和 Maven：

     ```powershell
     $env:JAVA_HOME="E:\java\jdk\jdk11"
     $env:MAVEN_HOME="D:\Java\apache-maven-3.8.6"
     $env:Path="$env:JAVA_HOME\bin;$env:MAVEN_HOME\bin;$env:Path"
     ```

   - 更新并编译：

     ```powershell
     git pull origin main
     mvn -pl consumer -am clean install -DskipTests
     ```

   - 启动 consumer：

     ```powershell
     mvn -pl consumer spring-boot:run `
       "-Dspring-boot.run.main-class=com.wxy.rpc.consumer.ConsumerApplication" `
       "-Dspring-boot.run.arguments=--rpc.client.registry-addr=127.0.0.1:12181 --rpc.client.serialization=PROTOSTUFF --rpc.client.load-balance=random --rpc.client.timeout=30000 --rpc.client.retries=0 --logging.level.com.wxy.rpc=warn"
     ```
     ```

     ```
# 压力测试
   ## JMEter
  1. 打开软件：`D:\Java\apache-jmeter-5.6.3\bin\jmeter.bat`
   ## JME
    ```
      mvn test-compile -DskipTests
      mvn -pl consumer -DincludeScope=test dependency:build-classpath "-Dmdep.outputFile=target/test-cp.txt" -DskipTests
      $cp = "consumer\target\test-classes;consumer\target\classes;provider-api\target\classes;rpc-framework-core\target\classes;rpc-client-spring-boot\target\classes;rpc-client-spring-boot-starter\target\classes;" + (Get-Content consumer\target\test-cp.txt)
      $jvmArgs = @(
        "-Drpc.client.registry-addr=127.0.0.1:12181",
        "-Drpc.client.registryAddr=127.0.0.1:12181",
        "-Drpc.client.timeout=30000",
        "-Drpc.client.retries=0",
        "-Drpc.client.failure-threshold=2147483647",
        "-Drpc.client.slow-call-threshold-millis=600000",
        "-Drpc.client.slow-call-threshold-count=2147483647",
        "-Dlogging.level.root=error"
      )
    ```
