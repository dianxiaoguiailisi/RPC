package com.wxy.rpc.api.service;

import com.wxy.rpc.api.pojo.User;

import java.util.List;

/**
 * 用户服务接口。
 */
public interface UserService {

    User queryUser();

    List<User> getAllUsers();

}
