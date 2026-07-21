package com.wxy.rpc.consumer.controller;

import com.wxy.rpc.api.service.BenchmarkService;
import com.wxy.rpc.client.annotation.RpcReference;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 压测接口入口，供 JMeter 做 HTTP 端到端压测。
 */
@RestController
@RequestMapping("/benchmark")
public class BenchmarkController {

    @RpcReference
    private BenchmarkService benchmarkService;

    @RequestMapping("/ping/{name}")
    public String ping(@PathVariable("name") String name) {
        return benchmarkService.ping(name);
    }

    @RequestMapping("/cpu/{rounds}")
    public String cpu(@PathVariable("rounds") Integer rounds) {
        return benchmarkService.cpuTask(rounds);
    }

    @RequestMapping("/io/{millis}")
    public String io(@PathVariable("millis") Integer millis) {
        return benchmarkService.ioTask(millis);
    }
}
