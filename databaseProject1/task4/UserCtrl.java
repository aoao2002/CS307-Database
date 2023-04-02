import com.google.gson.Gson;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class UserCtrl {

/**
 * root无限制
 * salesman可查看自己相关订单所有信息
 * directer 可查看自己相关订单所有信息，并修改/增加/删除 属于自己的order
 * 初始的Nothing无权限
 * 将number/name作为用户名
 * 相同type不可重名
 * **/
private HashSet<User> users=new HashSet<>();

private User nowUser=null;

public User getUser(String username, String type){
    //是否user已存在
    //System.out.println(users.size());
    for (User index:users) {
        if(index.getUserName().equals(username)
                && index.getType().equals(type)){
            return index;
        }
    }
    return null;
}

public boolean login(String username, String password, String type){
    //是否user已存在
    User nextUser=getUser(username,type);
    if(nextUser==null) {
        System.out.println("try to login an nonexistent user "+username+" "+type);
        return false;
    }
    nowUser=nextUser;
    System.out.println("already login as"+nowUser.getUserName()+" "+nowUser.getType());
    return true;
    //登录当前用户
}

public User createUser(String username, String password, String type,User root) {
    if(getUser(username,type)!=null) {
        System.out.println("create user with existed username(type)!");
        System.exit(0);
    }
    User newUser =root.makeUser(new User(username,password),type);
    if(newUser==null) {
        System.out.println("can't create user with name: "+username+" "+type);
        System.exit(0);
    }
    users.add(newUser);
    return newUser;
}

public boolean logout(){
    nowUser=new User("Nothing","Nothing");
    return true;
}

public void  loadUsersFromJson(){
    Gson gson = new Gson();
    Path path = new File("users.json").toPath();
    try (Reader reader = Files.newBufferedReader(path,
            StandardCharsets.UTF_8)) {
        this.users = gson.fromJson(reader, UserCtrl.class).users;
    }catch (Exception e){
        e.printStackTrace();
    }
}

public void Save2Json(){
    Gson gson = new Gson();
    try (FileOutputStream fos = new FileOutputStream("order.json");
         OutputStreamWriter isr = new OutputStreamWriter(fos,
                 StandardCharsets.UTF_8)) {
        gson.toJson(this, isr);
    }catch (Exception e){
        e.printStackTrace();
    }
}

public User getNowUser(){
    return nowUser;
}

public UserCtrl(){
    //获取已有用户的信息
    //this.users= loadUsersFromJson().users;
    this.users.add(User.getRoot("CS307,yyds"));
}



}

class User{
    private String type;
    private String userName;
    private String password;

    public User(String userName, String password){
        this.userName=userName;
        this.password=password;
        this.type="Nothing";
    }

    public String getType(){
        return type;
    }
    public String getUserName(){
        return userName;
    }
    /**
     * 只能由root用户注册其他用户
     * **/
    public User makeUser(User unLoginUser, String targetType){
        if(!this.type.equals("root")) return null;
        unLoginUser.type=targetType;
        return unLoginUser;
    }
    /**
     * 返回初始的root用户，要求输入正确的密码
     * 密码错误返回null
     * **/
    public static User getRoot(String password){
        if(password.equals("CS307,yyds")){
            User root = new User("iniRoot","CS307,yyds");
            root.type="root";
            return root;
        }
        return null;
    }
}