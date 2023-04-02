import com.google.gson.Gson;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.*;

public class FileManipulation  extends DataManipulation{

    private JsonDataCtrl jsonDataCtrl;
    private String dataRoot="./";

    @Override
    public void openDatasource() {
        super.openDatasource();
        this.jsonDataCtrl = new JsonDataCtrl();
        File file = new File("order.json");
        if (!file.exists()) jsonDataCtrl.loadScv("contract_info.csv");
        else this.jsonDataCtrl=this.jsonDataCtrl.loadJson();
        //Gson gson=new Gson();
        //System.out.println(gson.toJson(this.jsonDataCtrl.orderList.get(0)));
        //System.out.println(gson.toJson(this.jsonDataCtrl.orderList.get(5)));
        //System.out.println(gson.toJson(this.jsonDataCtrl.orderList.get(50000-1)));
    }

    @Override
    public void closeDatasource() {
        super.closeDatasource();
        jsonDataCtrl.save2Json();
        jsonDataCtrl.userCtrl.logout();
    }

    public int addOneOrder(String modelCode,String lodgementDate,
                           String estimatedDeliveryDate,int saleManNum,
                           int quanlity,
                           String contractNum){
        super.addOneOrder(modelCode,lodgementDate,estimatedDeliveryDate,saleManNum,quanlity,contractNum);
        return this.jsonDataCtrl.addOrder(modelCode,lodgementDate,estimatedDeliveryDate,saleManNum,
                quanlity,contractNum).id;
    }

    public boolean removeOneOrder(int id, String contractNum) {
        super.removeOneOrder(id,contractNum);
        int size=this.jsonDataCtrl.orderList.size();
        Order index;
        for (int i = 0; i < size; i++) {
            index=this.jsonDataCtrl.orderList.get(i);
            if(index.id==id && index.contractNum.equals(contractNum)){
                this.jsonDataCtrl.orderList.remove(i);
                return true;
            }
        }
        return false;
    }

    public int queryMaxOrderId(String conNum){
        super.queryMaxOrderId(conNum);
        return this.jsonDataCtrl.findMaxOrderId(conNum);
    }

    public ArrayList<String> queryAllOrder(){
        super.queryAllOrder();
        ArrayList<String> output = new ArrayList<>();
        int size=this.jsonDataCtrl.orderList.size();
        Order index;
        //System.out.println("query order size: "+size);
        for (int i = 0; i < size; i++) {
            index=this.jsonDataCtrl.orderList.get(i);
            output.add(index.id+"");
            output.add(index.estimatedDeliveryDate);
            output.add(index.lodgementDate);
            output.add(index.quantity+"");
            output.add(index.modelName);
            output.add(index.saleManNum+"");
            output.add(index.contractNum);
        }
        return output;
    }

    /**
     * 找到这个product的所有order
     * @param productName
     * @return
     */
    public ArrayList<String> queryOrderByProduct(String productName){
        super.queryOrderByProduct(productName);
        int size=this.jsonDataCtrl.orderList.size();
        ArrayList<String> output = new ArrayList<>();
        Order index;
        Model nowModel;
        Product nowProduct;

        for (int i = 0; i < size; i++) {
            index=this.jsonDataCtrl.orderList.get(i);
            nowModel= this.jsonDataCtrl.findModel(index.modelName);
            nowProduct=this.jsonDataCtrl.findProduct(nowModel.productCode);
            if(nowProduct.name.equals(productName)){
                output.add(index.id+"");
                output.add(index.contractNum);
            }
        }
        return output;
    }

    public HashMap<String,Integer> countOrderByCountry(){
        super.countOrderByCountry();
        HashMap<String,Integer> output = new HashMap<>();
        int size=this.jsonDataCtrl.orderList.size();
        Order index;
        contract nowcontract;
        ClintEnterprise nowCE;
        Location nowLocation;
        for (int i = 0; i < size; i++) {
            index=jsonDataCtrl.orderList.get(i);
            nowcontract=jsonDataCtrl.findcontract(index.contractNum);
            nowCE=jsonDataCtrl.findClintEnterprise(nowcontract.clintEnterpriseName);
            nowLocation= jsonDataCtrl.findLocation(nowCE.location_id);
            output.putIfAbsent(nowLocation.country,0);
            output.replace(nowLocation.country,output.get(nowLocation.country)+1);
        }
        return output;
    }

    /**
     * 如果left>right --> 1
     * == --> 0
     * < --> -1
     * @param left
     * @param right
     * @return
     */
    public int timeCompare(String left, String right){

        String[] leftTime = left.split("-");
        String[] rightTime = right.split("-");
        if(leftTime.length!=3||rightTime.length!=3){
            System.out.println("error! wrong time was compare: "+left+" and "+right);
            System.exit(0);
        }
        int output=0;
        for (int i = 0; i < 3; i++) {
            if(Integer.parseInt(leftTime[i])<Integer.parseInt(rightTime[i])){
                output=-1;
                break;
            }
            if(Integer.parseInt(leftTime[i])>Integer.parseInt(rightTime[i])){
                output=1;
                break;
            }
        }
        return output;
    }

    public ArrayList<String> queryOrderByEDdate(String begin, String end){
        super.queryOrderByEDdate(begin,end);
        int size=this.jsonDataCtrl.orderList.size();
        ArrayList<String> output=new ArrayList<>();
        Order index;
        for (int i = 0; i < size; i++) {
            index=this.jsonDataCtrl.orderList.get(i);
            if(
                    timeCompare(begin,index.estimatedDeliveryDate)<=0 &&
                    timeCompare(index.estimatedDeliveryDate,end)<=0
            ){
                output.add(index.id+"");
                output.add(index.contractNum);
            }

        }
        return output;
    }

    public void alterQuantityByOne(){
        super.alterQuantityByOne();
        int size=this.jsonDataCtrl.orderList.size();
        Order index;
        for (int i = 0; i < size; i++) {
            index=this.jsonDataCtrl.orderList.get(i);
            index.quantity++;
        }
    }

    public void removeOrderBefore(String lodgementDate){
        super.removeOrderBefore(lodgementDate);
        int size=this.jsonDataCtrl.orderList.size();
        Order index;
        //the value of lodgement date will be NULL if the date is later than 2022-3-2
        ArrayList<Order> newOrderList=new ArrayList<>();
        for (int i = 0; i < size; i++) {
            index=this.jsonDataCtrl.orderList.get(i);
            if(index.lodgementDate.equals("")) continue;
            if(timeCompare(index.lodgementDate, lodgementDate)>0)
                newOrderList.add(index);
        }
        this.jsonDataCtrl.orderList=newOrderList;
    }

    public String getDirectByCon(String conNum){
        contract nowContract= jsonDataCtrl.findcontract(conNum);
        ClintEnterprise nowCE = jsonDataCtrl.findClintEnterprise(nowContract.clintEnterpriseName);
        return jsonDataCtrl.findSupplyCenter(nowCE.SupplyCenter).director;
    }

    public ArrayList<String> queryDirectSelfOrder(String name){
        super.queryDirectSelfOrder(name);
        ArrayList<String> output=new ArrayList<>();
        int size=this.jsonDataCtrl.supplyCenterList.size();
        SupplyCenter nowSC=null;
        boolean flag=false;
        for (int i = 0; i < size; i++) {
            nowSC=jsonDataCtrl.supplyCenterList.get(i);
            if(nowSC.director.equals(name)){
                flag=true;
                break;
            }
        }
        if(!flag) {
            System.out.println("error in queryDirectSelfOrder without exist direct: "+name);
            System.exit(0);
        }
        size=jsonDataCtrl.orderList.size();
        Order nowOrder;
        contract nowCon ;
        ClintEnterprise nowCE;
        String SupplyCenterName= nowSC.name;
        for (int i = 0; i < size; i++) {
            nowOrder=jsonDataCtrl.orderList.get(i);
            nowCon= jsonDataCtrl.findcontract(nowOrder.contractNum);
            nowCE= jsonDataCtrl.findClintEnterprise(nowCon.clintEnterpriseName);
            if(nowCE.SupplyCenter.equals(SupplyCenterName)){
                output.add(nowOrder.id+"");
                output.add(nowOrder.contractNum);
            }
        }
        return output;
    }

    public ArrayList<String> querySalesmanSelfOrder(int number){
        super.querySalesmanSelfOrder(number);
        ArrayList<String> output=new ArrayList<>();
        int size=this.jsonDataCtrl.salesmenList.size();
        Salesman nowSalesMan=null;
        boolean flag=false;
        for (int i = 0; i < size; i++) {
            nowSalesMan=jsonDataCtrl.salesmenList.get(i);
            if(nowSalesMan.number==number){
                flag=true;
                break;
            }
        }
        if(!flag) {
            System.out.println("error in querySalesmanSelfOrder without exist direct: "+number);
            System.exit(0);
        }
        size=jsonDataCtrl.orderList.size();
        Order nowOrder;
        for (int i = 0; i < size; i++) {
            nowOrder=jsonDataCtrl.orderList.get(i);
            if(number==nowOrder.saleManNum){
                output.add(nowOrder.id+"");
                output.add(nowOrder.contractNum);
            }
        }
        return output;
    }
}
