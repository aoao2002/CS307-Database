import com.google.gson.Gson;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;


public class JsonDataCtrl {
    private int newDataSize=950000;
    public ArrayList<Model> modelList=new ArrayList<>();
    public ArrayList<Product> productList=new ArrayList<>();
    public ArrayList<Salesman> salesmenList=new ArrayList<>();
    public ArrayList<Location> locationList=new ArrayList<>();
    public ArrayList<ClintEnterprise> clintEnterpriseList=new ArrayList<>();
    public ArrayList<contract> contractList=new ArrayList<>();
    public ArrayList<Order> orderList=new ArrayList<>();
    public ArrayList<SupplyCenter> supplyCenterList=new ArrayList<>();
    public UserCtrl userCtrl=new UserCtrl();
    public String dataroot="";

    public Model addModel(String modelName,int unitPrice,String productCode){
        //检查主键
        Model index=findModel(modelName);
        if(findProduct(productCode)!=null && index==null) {
            index = new Model(modelName,unitPrice,productCode);
            this.modelList.add(index);
        }
        return index;
    }

    public Location addLocation(String cityName,String country){
        boolean ifAppear=false;
        Location index=findLocation(cityName,country);
        if(index==null){
            index = new Location(cityName,country);
            this.locationList.add(index);
        }
        return index;
    }

    public SupplyCenter addSupplyCenter(String name, String director){
        SupplyCenter index=findSupplyCenter(name);

        if(index==null) {
            index = new SupplyCenter(name, director);
            supplyCenterList.add(index);
        }
        return index;

    }

    public Salesman addSalesmen( int number,String name,String gender,
                                int age,String mobilePhone){
        Salesman index=findSalesman(number);

        if(index==null) {
            index = new Salesman(number, name, gender, age, mobilePhone);
            salesmenList.add(index);
        }
        return index;
    }

    public ClintEnterprise addClientEnterprise(String name,int location_id,
                                       String industry,String supplyCenter){

        ClintEnterprise index=findClintEnterprise(name);
        if(index==null){
            index = new ClintEnterprise(name,location_id,industry,supplyCenter);
            clintEnterpriseList.add(index);
        }
        return index;
    }

    public Product addProduct(String productCode,String name){
        //检查是否出现过即可
        Product index=findProduct(productCode);

        if(index==null){
            index=new Product(productCode,name);
            productList.add(index);
        }
        return index;
    }

    public Order addOrder(String modelName,
                          String lodgementDate,
                          String estimatedDeliveryDate,
                          int saleManNum,
                          int quantity,
                          String contractNum){
        //检查contractNum
        if(findcontract(contractNum)==null) {
            System.out.println("error! Addorder without exist contractNum: "+contractNum);
            System.exit(0);
        }
        //检查modelName
        if(findModel(modelName)==null){
            System.out.println("error! Addorder without exist modelName: "+modelName);
            System.exit(0);
        }
        //检查saleManNum
        if(findSalesman(saleManNum)==null){
            System.out.println("error! Addorder without exist saleManNum: "+saleManNum);
            System.exit(0);
        }

        Order index=new Order(findMaxOrderId(contractNum)+1,modelName,
                                lodgementDate,estimatedDeliveryDate,
                                saleManNum,quantity,contractNum);

        orderList.add(index);
        return index;
    }

    public contract addcontract(String num,String date,String clintEnterpriseName){
        boolean ifAppear=false;
        contract index=null;
        for (int i = 0; i < contractList.size(); i++) {
            index=contractList.get(i);
            if(index.num.equals(num)) {
                ifAppear=true;
                break;
            }
        }
        if(!ifAppear){
            index=new contract(num,date,clintEnterpriseName);
            contractList.add(index);
        }
        return index;
    }

    public Order findOrder(int id, String contractNum){
        int size=this.orderList.size();
        Order index;
        for (int i = 0; i < size; i++) {
            index=this.orderList.get(i);
            if(index.id==id && index.contractNum.equals(contractNum)){
                return this.orderList.get(i);
            }
        }
        return null ;
    }

    public Product findProduct(String productCode){
        int size=this.productList.size();
        for (int i = 0; i < size; i++) {
            if(this.productList.get(i).code.equals(productCode)){
                return this.productList.get(i);
            }
        }
        return null ;
    }

    public Model findModel(String name){
        int size=this.modelList.size();
        for (int i = 0; i < size; i++) {
            if(this.modelList.get(i).name.equals(name)){
                return this.modelList.get(i);
            }
        }
        return null ;
    }

    public int findMaxOrderId(String conNum){
//        int size=this.orderList.size();
//        int max=-1;
//        Order temp=null;
//        for (int i = 0; i < size; i++){
//            temp=this.orderList.get(i);
//            if(temp.id>max && temp.contractNum.equals(conNum)) max=temp.id;
//        } 10000ms->4000ms
        int size=this.contractList.size();
        contract temp;
        for (int i = 0; i < size; i++) {
            temp=contractList.get(i);
            if(temp.num.equals(conNum)) return temp.orderNum;
        }
        return -1;
    }

    public Salesman findSalesman(int saleManNum){
        int size=this.salesmenList.size();
        for (int i = 0; i < size; i++) {
            if(this.salesmenList.get(i).number==saleManNum){
                return this.salesmenList.get(i);
            }
        }
        return null ;
    }

    public contract findcontract(String contractNum){
        int size=this.contractList.size();
        for (int i = 0; i < size; i++) {
            if(this.contractList.get(i).num.equals(contractNum)){
                this.contractList.get(i).orderNum++;
                return this.contractList.get(i);
            }
        }
        return null;
    }

    public Location findLocation(String cityName,String country){
        int size=this.locationList.size();
        Location index;
        for (int i = 0; i < size; i++) {
            index=this.locationList.get(i);
            if(index.cityName.equals(cityName) && index.country.equals(country)){
                return index;
            }
        }
        return null;
    }

    public Location findLocation(int id){
        int size=this.locationList.size();
        Location index;
        for (int i = 0; i < size; i++) {
            index=this.locationList.get(i);
            if(index.id==id){
                return index;
            }
        }
        return null;
    }

    public SupplyCenter findSupplyCenter(String name){
        int size=this.supplyCenterList.size();
        SupplyCenter index;
        for (int i = 0; i < size; i++) {
            index=this.supplyCenterList.get(i);
            if(index.name.equals(name)){
                return index;
            }
        }
        return null;
    }

    public ClintEnterprise findClintEnterprise(String name){
        int size=this.clintEnterpriseList.size();
        ClintEnterprise index;
        for (int i = 0; i < size; i++) {
            index=this.clintEnterpriseList.get(i);
            if(index.name.equals(name)){
                return index;
            }
        }
        return null;
    }


    public boolean loadScv(String inputScv) {
        String fileName = inputScv;
        try (Scanner sc = new Scanner(new FileReader(fileName))) {
            //System.out.println("loadscv:");
            boolean firstLine=true;
            int cnt=0;
            while (sc.hasNextLine()) {  //按行读取字符串
                //cnt++;
                //System.out.println("read: "+cnt);
                String line = sc.nextLine();
                if(firstLine){
                    firstLine=false;
                    continue;
                }
                String[] order_ifo=line.split(",");

                Product nextProduct=addProduct(order_ifo[6],order_ifo[7]);

                Model nextModel=addModel(order_ifo[8],Integer.parseInt(order_ifo[9].strip()),nextProduct.code);

                Salesman nextSalesman=addSalesmen(Integer.parseInt(order_ifo[16]),order_ifo[15],
                        order_ifo[17],Integer.parseInt(order_ifo[18]),order_ifo[19]);

                SupplyCenter nextSupplyCenter=addSupplyCenter(order_ifo[2],order_ifo[14]);

                Location nextLocation=addLocation(order_ifo[4],order_ifo[3]);

                ClintEnterprise nextCE=addClientEnterprise(order_ifo[1],nextLocation.id,
                                                           order_ifo[5],nextSupplyCenter.name);

                contract nextCon=addcontract(order_ifo[0],order_ifo[11], nextCE.name);

                addOrder(nextModel.name,
                        order_ifo[13],
                        order_ifo[12],
                        nextSalesman.number,
                        Integer.parseInt(order_ifo[10].strip()),
                        nextCon.num);
//                addOrder(addProduct(order_ifo[6],order_ifo[7],
//                        addModel(order_ifo[8],Integer.parseInt(order_ifo[9].strip())).id).code,
//                        order_ifo[13],
//                        order_ifo[12],
//                        addSalesmen(Integer.parseInt(order_ifo[16]),order_ifo[15],
//                                    order_ifo[17],Integer.parseInt(order_ifo[18]),order_ifo[19]).number,
//                        Integer.parseInt(order_ifo[10].strip()),
//                        addcontract(order_ifo[0],order_ifo[11],
//                                addClientEnterprise(order_ifo[1],
//                                                    addLocation(order_ifo[4],order_ifo[3]).id,order_ifo[5],
//                                                    addSupplyCenter(order_ifo[2],order_ifo[14]).name
//                                                   ).name).num);


            }
        }catch (IOException ioe){
            ioe.printStackTrace();
        }
        return true;
    }

    public boolean save2Json(){
        Gson gson = new Gson();
        try (FileOutputStream fos = new FileOutputStream(this.dataroot+"order.json");
             OutputStreamWriter isr = new OutputStreamWriter(fos,
                     StandardCharsets.UTF_8)) {
            gson.toJson(this, isr);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public JsonDataCtrl loadJson(){
        //System.out.println("in loadjson");
        Gson gson = new Gson();
        Path path = new File(this.dataroot+"order.json").toPath();
        JsonDataCtrl jsonDataCtrl=new JsonDataCtrl();
        try (Reader reader = Files.newBufferedReader(path,
                StandardCharsets.UTF_8)) {
            jsonDataCtrl = gson.fromJson(reader, JsonDataCtrl.class);
        }catch (Exception e){
            e.printStackTrace();
        }
        return jsonDataCtrl;
    }

    public void makeOrderData(){
        ArrayList<String> newData=new ArrayList<>();
        Random random=new Random();
        int randNum=0;
        Order index;
        int newDataNum=this.newDataSize;
        for (int i = 0; i < newDataNum; i++) {
            randNum=random.nextInt(49999);
            index=orderList.get(randNum);
            newData.add(index.modelName);
            randNum=random.nextInt(49999);
            index=orderList.get(randNum);
            newData.add(index.lodgementDate);
            randNum=random.nextInt(49999);
            index=orderList.get(randNum);
            newData.add(index.estimatedDeliveryDate);
            randNum=random.nextInt(49999);
            index=orderList.get(randNum);
            newData.add(index.saleManNum+"");
            randNum=random.nextInt(49999);
            index=orderList.get(randNum);
            newData.add(index.quantity+"");
            randNum=random.nextInt(49999);
            index=orderList.get(randNum);
            newData.add(index.contractNum);
        }
        Gson gson=new Gson();
        try (FileOutputStream fos = new FileOutputStream(this.dataroot+"newData.json");
             OutputStreamWriter isr = new OutputStreamWriter(fos,StandardCharsets.UTF_8)) {
            gson.toJson(newData, isr);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 为所有director/salesman创建用户、
     * 初始密码与用户名相同
     */
    public  void createUsers(){
        UserCtrl userCtrl=new UserCtrl();
        User root = User.getRoot("CS307,yyds");
        //System.out.println(root.getType());
        int size=supplyCenterList.size();
        SupplyCenter supplyCenter;
        for (int i = 0; i < size; i++) {
            supplyCenter=supplyCenterList.get(i);
            userCtrl.createUser(supplyCenter.director, supplyCenter.director,"director" ,root);
        }
        size = salesmenList.size();
        Salesman salesman;
        for (int i = 0; i < size; i++) {
            salesman=salesmenList.get(i);
            userCtrl.createUser(salesman.number+"",salesman.number+"","salesman",root);
        }

        Gson gson = new Gson();
        try (FileOutputStream fos = new FileOutputStream(this.dataroot+"users.json");
             OutputStreamWriter isr = new OutputStreamWriter(fos,
                     StandardCharsets.UTF_8)) {
            gson.toJson(userCtrl, isr);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        JsonDataCtrl jsonDataCtrl=new JsonDataCtrl();
        jsonDataCtrl.loadScv("contract_info.csv");
        jsonDataCtrl.makeOrderData();
        //jsonDataCtrl.createUsers();
    }
}

class Model{
    String name;
    String productCode;
    int unitPrice;
    Model(String name,int unitPrice,String productCode)
    {
        this.name=name;
        this.unitPrice=unitPrice;
        this.productCode=productCode;
    }
}

class Product {
    //pk
    String code;
    String name;

    Product(String code, String name)
    {
        this.code=code;
        this.name=name;
    }

}

class Salesman{
    int number;
    String name;
    String gender;
    int age;
    String mobilePhone;
    Salesman(int number, String name, String gender, int age, String mobilePhone)
    {
        this.number=number;
        this.name=name;
        this.gender=gender;
        this.age=age;
        this.mobilePhone=mobilePhone;
    }
}

class Location{
    static int num=0;
    int id;
    String cityName;
    String country;
    Location(String cityName, String country)
    {
        this.id=num;
        num++;
        this.cityName=cityName;
        this.country=country;
    }
}

class ClintEnterprise{
    String name;
    int location_id;
    String industry;
    String SupplyCenter;
    ClintEnterprise(String name, int location_id,String industry, String SupplyCenter){
        this.name=name;
        this.location_id=location_id;
        this.industry=industry;
        this.SupplyCenter=SupplyCenter;
    }
}

class contract{
    int orderNum;
    String num;
    String date;
    String clintEnterpriseName;
    contract(String num,String date,String clintEnterpriseName)
    {
        this.orderNum=-1;
        this.num=num;
        this.date=date;
        this.clintEnterpriseName=clintEnterpriseName;
    }
}

class Order{
    int id;//主键
    String modelName;
    String lodgementDate;
    String estimatedDeliveryDate;
    int saleManNum;
    int quantity;
    String contractNum;
    Order(  int id,
            String modelName,
            String lodgementDate,
            String estimatedDeliveryDate,
            int saleManNum,
            int quantity,
            String contractNum){
        this.modelName=modelName;
        this.id=id;
        this.lodgementDate=lodgementDate;
        this.estimatedDeliveryDate=estimatedDeliveryDate;
        this.saleManNum=saleManNum;
        this.quantity=quantity;
        this.contractNum=contractNum;
    }
}

class SupplyCenter {
    String name;
    String director;
    SupplyCenter(String name,String director){
        this.name=name;
        this.director=director;
    }
}

