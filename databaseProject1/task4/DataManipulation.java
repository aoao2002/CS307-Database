import java.util.ArrayList;
import java.util.HashMap;

public abstract class DataManipulation {
    private UserCtrl userCtrl = new UserCtrl();
    private boolean ifOpenUserPrivilegesPrivilegesPrivileges=true;

    boolean login(String name, String password, String type){
        return userCtrl.login(name, password, type);
    }

    /**
     * 连接
     */
     void openDatasource(){
         userCtrl.loadUsersFromJson();
         //userCtrl.login("iniroot","root","root");
     }
      void closeDatasource(){
         userCtrl.logout();
         userCtrl.Save2Json();
      }

    /**
     *增
     */
    int addOneOrder(String modelCode,String logementDate,
                    String estimatedDeliveryDate,int saleManNum,
                    int quanlity,
                    String contractNum){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return 0;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root")) &&
                (!nowUser.getType().equals("director")) ) {

            System.out.println("using addOneOrder without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        if(nowUser.getType().equals("root")) return 0;
        if(!getDirectByCon(contractNum).equals(nowUser.getUserName())){
            System.out.println("using addOneOrder not for self without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        return 0;
    }

    /**
     * 删除
     */
    boolean removeOneOrder(int id, String contractNum){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return false;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))&&
                (!nowUser.getType().equals("director")) ) {
            System.out.println("using removeOneOrder without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        if(nowUser.getType().equals("root")) return false;
        if(!getDirectByCon(contractNum).equals(nowUser.getUserName())){
            System.out.println("using removeOneOrder not for self without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        return true;
    }
    void removeOrderBefore(String lodgementDate){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return ;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))) {
            System.out.println("using removeOrderBefore without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }

    }

    /**
     * 改
     */

    /**
     * 将每个订单数量加1
     */
    void alterQuantityByOne(){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return ;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))) {
            System.out.println("using alterQuantityByOne without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
    }


    // 查
    ArrayList<String> queryAllOrder(){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return null;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))) {
            System.out.println("using alterQuantityByOne without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        return null;
    }

    abstract String getDirectByCon(String conNum);

    ArrayList<String> queryDirectSelfOrder(String name){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return null;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))&&
                (!nowUser.getType().equals("director")) ) {
            System.out.println("using queryDirectSelfOrder WITHOUT privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }

        return null;
    }

    ArrayList<String> querySalesmanSelfOrder(int number){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return null;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))&&
                (!nowUser.getType().equals("salesman")) ) {
            System.out.println("using querySalesmanSelfOrder WITHOUT privileges with type and num: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }

        return null;
    }


    /**
     * 通过型号查询订单，返回带有orderId的Arraylist
     * @param productName
     * @return
     */
    ArrayList<String> queryOrderByProduct(String productName){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return null;
        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))) {
            System.out.println("using queryOrderByProduct without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        return null;
    }


    /**
     * 通过日期范围查询订单estimated delivery date
     * 要求左闭右闭
     * @param begin
     * @param end
     * @return
     */
    ArrayList<String> queryOrderByEDdate(String begin, String end){
        if(!ifOpenUserPrivilegesPrivilegesPrivileges) return null;

        User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))) {
            System.out.println("using queryOrderByEDdate without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        return null;
    }


    /**
     * 查询每个国家的订单总数
     * @return
     */
     HashMap<String,Integer> countOrderByCountry(){
         if(!ifOpenUserPrivilegesPrivilegesPrivileges) return null;

         User nowUser = userCtrl.getNowUser();
         if((!nowUser.getType().equals("root"))) {
             System.out.println("using countOrderByCountry without privileges with type and name: "
                     +nowUser.getType()+" "+ nowUser.getUserName());
             System.exit(0);
         }
         return null;
    }

     int queryMaxOrderId(String conNum){
         if(!ifOpenUserPrivilegesPrivilegesPrivileges) return 0;

         User nowUser = userCtrl.getNowUser();
        if((!nowUser.getType().equals("root"))) {
            System.out.println("using countOrderByCountry without privileges with type and name: "
                    +nowUser.getType()+" "+ nowUser.getUserName());
            System.exit(0);
        }
        return 0;
    }


}
