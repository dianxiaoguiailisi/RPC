package com.wxy.rpc.consumer.controller;

import com.wxy.rpc.api.pojo.User;
import com.wxy.rpc.api.service.UserService;
import com.wxy.rpc.client.annotation.RpcReference;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class UserController {
    //通过@RpcReference注入远程代理对象
    @RpcReference
    private UserService userService;


    @RequestMapping("/user/getUser")
    public User getUser() {

        return userService.queryUser();
    }

    @RequestMapping("/user/getAllUser")
    public List<User> getAllUser() {

        return userService.getAllUsers();
    }

}
