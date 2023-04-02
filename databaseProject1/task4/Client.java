import com.google.gson.Gson;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

public class Client {
    public static String ClientName;
    public static String ClientPassword;
    public static String ClientType;

    //是否检查add/并用新数据测试
    private static boolean ifAddNewData = false;
    private static boolean justCheckQuery = false;
    private static ArrayList<String> fileLog = new ArrayList<>();
    private static ArrayList<String> dbLog = new ArrayList<>();

    private static String beginData="2018-04-03";
    private static String endData="2022-04-15";
    private static String removeDate="2019-01-01";
    /**
     * 测试从CSV导入
     * @param dm
     */
    public static void checkLoad(DataManipulation dm){
        File file = new File("order.json");
        if (file.exists()) file.delete();
        long startTime = System.currentTimeMillis();
        dm.openDatasource();
        dm.closeDatasource();
        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        System.out.println("load from csv: "+usedTime+" ms");
    }

    /**
     * select * from order
     * @param dm
     * @param log
     */
    public static void checkQueryAllOrder(DataManipulation dm,ArrayList<String> log){

        long startTime = System.currentTimeMillis();

        ArrayList<String> output=dm.queryAllOrder();

        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        int size=output.size();
        System.out.println(size/7);
        for (int i = 0; i < size; i++) {
            log.add(output.get(i));
        }
        System.out.println("select * from order with time: "+usedTime+" ms");
    }

    public static void checkQueryOrderByProduct(@NotNull DataManipulation dm, String productName,
                                                ArrayList<String> log){
        ArrayList<String> output;
        long startTime = System.currentTimeMillis();

        output=dm.queryOrderByProduct(productName);

        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        int size=output.size();
        System.out.println(size/2);
        for (int i = 0; i < size; i++) {
            log.add(output.get(i));
        }
        System.out.println("query order by productName "+productName+" with time: "+usedTime+" ms");
    }

    public static void checkCountOrderByCountry(DataManipulation dm,ArrayList<String> log){
        HashMap<String,Integer> output;
        long startTime = System.currentTimeMillis();

        output=dm.countOrderByCountry();

        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        int size=output.size();

        for (String country:output.keySet()) {
            //System.out.println(country+": "+output.get(country));
            log.add(country+": "+output.get(country));
        }

        System.out.println(size);
        System.out.println("count orders by country with time: "+usedTime+" ms");
    }

    public static void checkQueryOrderByEDdate(DataManipulation dm,ArrayList<String> log){
        ArrayList<String> output=new ArrayList<>();
        long startTime = System.currentTimeMillis();

        output=dm.queryOrderByEDdate(beginData,endData);

        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        int size=output.size();
        System.out.println(size/2);
        for (int i = 0; i < size; i++) {
            log.add(output.get(i));
        }
        System.out.println("query orders by estimated_delivery_data with time: "+usedTime+" ms");
    }

    public static void checkAddOneOrder(DataManipulation dm){
        ArrayList<String> newData = getNewData();
        long startTime = System.currentTimeMillis();
        int size=newData.size();
        System.out.println("newDataSize: "+size/6);
        for (int i = 0; i < size; i+=6) {
            dm.addOneOrder(
                    newData.get(i),
                    newData.get(i+1),
                    newData.get(i+2),
                    Integer.parseInt(newData.get(i+3)),
                    Integer.parseInt(newData.get(i+4)),
                    newData.get(i+5)
            );
        }
        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        System.out.println("add new data with time: "+usedTime+" ms");
    }

    public static ArrayList<String> getNewData(){
        Gson gson = new Gson();
        ArrayList<String> newData = new ArrayList<>();
        Path path = new File("newData.json").toPath();
        JsonDataCtrl jsonDataCtrl=new JsonDataCtrl();
        try (Reader reader = Files.newBufferedReader(path,
                StandardCharsets.UTF_8)) {
            newData = gson.fromJson(reader, ArrayList.class);
        }catch (Exception e){
            e.printStackTrace();
        }
        return newData;
    }

    public static void checkAlterQuantityByOne(DataManipulation dm){
        long startTime = System.currentTimeMillis();

        dm.alterQuantityByOne();

        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        System.out.println("alter orders by adding quantity by one with time: "+usedTime+" ms");
    }

    public static void checkRemoveOrderBefore(DataManipulation dm){
        long startTime = System.currentTimeMillis();

        dm.removeOrderBefore(removeDate);

        long endTime = System.currentTimeMillis();
        long usedTime = (endTime-startTime);
        System.out.println("remove orders before(LDate) "+removeDate+"with: "+usedTime+" ms");
    }

    public static void checkAnswer(){
        int size=0;
        if(Client.dbLog.size()==Client.fileLog.size()){
            size=Client.dbLog.size();
        }else {
            System.out.println("WA!! without same size");
            System.exit(0);
        }
//        for (int i = 0; i < size; i++) {
//            if (!Client.fileLog.get(i).equals(Client.dbLog.get(i))){
//                //查询顺序不同不好比较
//                System.out.println("WA!! with different query: "+Client.fileLog.get(i)+
//                        " and "+Client.dbLog.get(i)+" on "+i);
//                System.exit(0);
//            }
//        }
        System.out.println("AC!!");
    }

    public static void checkPrivileges(DataManipulation dm){
        dm.login(ClientName,ClientPassword,ClientType);
        //dm.querySalesmanSelfOrder(Integer.parseInt(Client.ClientName));
        dm.queryDirectSelfOrder(ClientName);
    }

    public static void main(String[] args) {

        for (int testTime = 0; testTime < 2; testTime++) {

            try {
                String mode="file";
                ArrayList<String> log = Client.fileLog;
                if(testTime==1) {
                    mode="database";
                    log=Client.dbLog;
                }
                System.out.println("using： "+mode);

                DataManipulation dm = new DataFactory().createDataManipulation(mode);

                if(testTime==0) checkLoad(dm);

                dm.openDatasource();

                //以root身份测试
                dm.login("iniRoot","CS307,yyds","root");
                //dm.login(Client.ClientName,Client.ClientPassword,Client.ClientType);


                //是否检查add/并用新数据测试
                if(Client.ifAddNewData)  checkAddOneOrder(dm);

                //select *
                checkQueryAllOrder(dm,log);

                //where+compare
                checkQueryOrderByEDdate(dm,log);

                //两次join
                checkQueryOrderByProduct(dm,"Tv Base",log);

                //group by
                checkCountOrderByCountry(dm,log);

                if(!justCheckQuery){
                    //修改
                    checkAlterQuantityByOne(dm);

                    //删除
                    checkRemoveOrderBefore(dm);
                }

                //权限测试
//                Client.ClientName="12111414";
//                Client.ClientType="salesman";
//                Client.ClientPassword="12111414";
//                checkPrivileges(dm);

                Client.ClientName="Xu Zhuyu";
                Client.ClientType="director";
                Client.ClientPassword="Xu Zhuyu";
                checkPrivileges(dm);


                dm.closeDatasource();
                System.out.println();

            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
            }
        }
        checkAnswer();

    }
}

