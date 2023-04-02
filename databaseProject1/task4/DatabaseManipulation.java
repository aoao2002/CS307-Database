
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;


public  class DatabaseManipulation extends DataManipulation {


    private Connection con = null;
    private HashMap<String,Integer> maxId=null;

    private String host = "localhost";
    private String dbname = "sustc";
    private String user = "checker";
    private String pwd = "123456";
    private String port = "3360";

    private String MaxIdSql="select max(id) from (\n" +
                            "select * from contract_order\n" +
                            "where contract_number=?) sub";

    private String AddOrderSql= "INSERT INTO contract_order(id,contract_number,estimated_delivery_date,lodgement_date," +
            "quantity,model,salesman_number)"
            +" values(?,?,to_date( ? ,'yyyy-mm-dd'),to_date( NULLIF(?,'') ,'yyyy-mm-dd')," +
            "to_number(?,'999999999'),?,to_number(?,'999999999'))";

    private String SelectConSql="select contract_number from contract_order";

    private String  SelectOrderSql="select * from contract_order";

    private String SelectOrderByProSql = "select id,contract_number from (( " +
            "    select id,contract_number,model_name,product_code " +
            "        from contract_order co join model m on co.model=m.model_name) sub1 " +
            "    join product p on sub1.product_code=p.code) " +
            "where product_name=? ";

    private String SelectOrderByCountrySql="select country,count(*) as cnt\n" +
            "from contract_order co\n" +
            "inner join (\n" +
            "    select number,country from contract c\n" +
            "inner join (\n" +
            "    select  name,country from client_enterprise\n" +
            "    inner join location l on client_enterprise.location_id = l.location_id\n" +
            "    ) sub1\n" +
            "on sub1.name=c.client_enterprise\n" +
            "    )sub2\n" +
            "on co.contract_number=sub2.number\n" +
            "group by country";

    private String SelectOrderByEDateSql="select id,contract_number from contract_order" +
            " where estimated_delivery_date>=? and "+
            "estimated_delivery_date<=?" ;

    private String addQuantityByOneSql="update contract_order co\n" +
            "set quantity=co.quantity+1";

    private String removeOrderBeforeSql="delete from contract_order co\n" +
            "where co.lodgement_date<=?";

    private String getDirectByConSql="select director_name from contract\n" +
            "inner join client_enterprise ce on ce.name = contract.client_enterprise\n" +
            "inner join supply_center sc on ce.supply_center = sc.center_name\n" +
            "where number=?";

    private String queryDirectSelfOrderSql ="select id,contract_number from contract_order\n" +
            "inner join (\n" +
            "select number from contract\n" +
            "inner join (\n" +
            "select name from client_enterprise\n" +
            "inner join (\n" +
            "select center_name from supply_center\n" +
            "where director_name=?) sub1 on client_enterprise.supply_center=sub1.center_name) sub2\n" +
            "on contract.client_enterprise=name) sub3\n" +
            "on contract_order.contract_number=sub3.number";

    private String querySalesmanSelfOrderSql="select id,contract_number,s.number from  contract_order\n" +
            "inner join salesman s on s.number = contract_order.salesman_number\n" +
            "where s.number=?";

    private PreparedStatement MaxIdStatement=null;
    private PreparedStatement AddOrderStatement=null;
    private PreparedStatement SelectConStatement=null;
    private PreparedStatement SelectOrderByProStatement=null;
    private PreparedStatement SelectOrderStatement=null;
    private PreparedStatement SelectOrderByCountryStatement=null;
    private PreparedStatement SelectOrderByEDateStatement=null;
    private PreparedStatement addQuantityByOneStatement=null;
    private PreparedStatement removeOrderBeforeStatement=null;
    private PreparedStatement getDirectByConStatement=null;
    private PreparedStatement queryDirectSelfOrderStatement=null;
    private PreparedStatement querySalesmanSelfOrderStatement=null;

    @Override
    public void openDatasource() {
        super.openDatasource();
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:mysql://" + host + "/" + dbname;
            //String url = "jdbc:mysql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }

        try {
            this.MaxIdStatement=con.prepareStatement(MaxIdSql);
            this.AddOrderStatement=con.prepareStatement(AddOrderSql);
            this.SelectConStatement=con.prepareStatement(SelectConSql);
            this.SelectOrderByProStatement=con.prepareStatement(SelectOrderByProSql);
            this.SelectOrderStatement=con.prepareStatement(SelectOrderSql);
            this.SelectOrderByCountryStatement=con.prepareStatement(SelectOrderByCountrySql);
            this. SelectOrderByEDateStatement=con.prepareStatement(SelectOrderByEDateSql);
            this.addQuantityByOneStatement=con.prepareStatement(addQuantityByOneSql);
            this. removeOrderBeforeStatement=con.prepareStatement(removeOrderBeforeSql);
            this.getDirectByConStatement=con.prepareStatement(getDirectByConSql);
            this.queryDirectSelfOrderStatement=con.prepareStatement(queryDirectSelfOrderSql);
            this.querySalesmanSelfOrderStatement=con.prepareStatement(querySalesmanSelfOrderSql);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closeDatasource() {
        super.closeDatasource();
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int addOneOrder(String modelCode,String logementDate,
                           String estimatedDeliveryDate,int saleManNum,
                           int quanlity,
                           String contractNum) {
        super.addOneOrder(modelCode,logementDate,estimatedDeliveryDate,saleManNum,quanlity,contractNum);
        int result = 0;

        int id=1;
        if(maxId==null) getMaxIdMap();
        if(maxId.putIfAbsent(contractNum,1)!=null) {
            id=maxId.get(contractNum)+1;
            maxId.replace(contractNum,id);
        }
        try {
            PreparedStatement preparedStatement = this.AddOrderStatement;

            preparedStatement.setInt(1, id);
            preparedStatement.setString(2, contractNum);

            //if(estimatedDeliveryDate.equals("")) preparedStatement.setNull(3,Types.DATE);
            //else preparedStatement.setDate(3, Date.valueOf(estimatedDeliveryDate));

            //if(logementDate.equals("")) preparedStatement.setNull(4, Types.DATE);
            //else preparedStatement.setDate(4, Date.valueOf(logementDate));

            preparedStatement.setString(3,estimatedDeliveryDate);
            preparedStatement.setString(4,logementDate);
            preparedStatement.setString(5, quanlity+"");
            preparedStatement.setString(6, modelCode);
            preparedStatement.setString(7, saleManNum+"");

            //System.out.println(preparedStatement.toString());
            preparedStatement.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean removeOneOrder(int id, String contractNum){
        super.removeOneOrder(id,contractNum);
        return true;
    }

    public ArrayList<String> findOrder(int id, String contractNum){

        String sql="select * from contract_order where id=to_number(?,'999999999') " +
                "and contract_number='?'";

        ArrayList<String> output=new ArrayList<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);

            preparedStatement.setString(1, id+"");
            preparedStatement.setString(2, contractNum);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()){
                output.add(resultSet.getString("id"));
                output.add(resultSet.getString("estimated_delivery_date"));
                output.add(resultSet.getString("lodgement_date"));
                output.add(resultSet.getString("quantity"));
                output.add(resultSet.getString("model"));
                output.add(resultSet.getString("salesman_number"));
                output.add(resultSet.getString("contract_number"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;
    }


    public int queryMaxOrderId(String conNum){
        super.queryMaxOrderId(conNum);
        int output=-1;
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = MaxIdStatement;

            preparedStatement.setString(1, conNum);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            if(!resultSet.getString("max").equals(""))
                output=Integer.parseInt(resultSet.getString("max"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;
    }

    public void getMaxIdMap(){

        ArrayList<String> contract=queryAllCon();
        int size=contract.size();
        String index;
        if(maxId==null) maxId=new HashMap<>();
        for (int i = 0; i < size; i++) {
            index=contract.get(i);
            maxId.put(index,queryMaxOrderId(index));
        }
    }

    public ArrayList<String> queryAllCon(){

        ArrayList<String> output=new ArrayList<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = SelectConStatement;
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()){
                output.add(resultSet.getString("contract_number"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;
    }

    public ArrayList<String> queryAllOrder(){
        super.queryAllOrder();
        ArrayList<String> output=new ArrayList<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = SelectOrderStatement;
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()){
                output.add(resultSet.getString("id"));
                output.add(resultSet.getString("estimated_delivery_date"));
                output.add(resultSet.getString("lodgement_date"));
                output.add(resultSet.getString("quantity"));
                output.add(resultSet.getString("model"));
                output.add(resultSet.getString("salesman_number"));
                output.add(resultSet.getString("contract_number"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;
    }

    public ArrayList<String> queryOrderByProduct(String productName){
        super.queryOrderByProduct(productName);
        ArrayList<String> output=new ArrayList<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = SelectOrderByProStatement;

            preparedStatement.setString(1, productName);
            //System.out.println(preparedStatement.toString());

            resultSet = preparedStatement.executeQuery();


            while (resultSet.next()){
                output.add(resultSet.getString("id"));
                output.add(resultSet.getString("contract_number"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;
    }

    public HashMap<String,Integer> countOrderByCountry(){

        super.countOrderByCountry();
        HashMap<String,Integer> output=new HashMap<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = SelectOrderByCountryStatement;

            //System.out.println(preparedStatement.toString());

            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()){
                output.put(resultSet.getString("country"),
                       Integer.parseInt(resultSet.getString("cnt"))) ;

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
       return output;
    }

    public ArrayList<String> queryOrderByEDdate(String begin, String end){

        super.queryOrderByEDdate(begin,end);
        ArrayList<String> output=new ArrayList<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = SelectOrderByEDateStatement;

            preparedStatement.setDate(1, Date.valueOf(begin));
            preparedStatement.setDate(2, Date.valueOf(end));
            //System.out.println(preparedStatement.toString());

            resultSet = preparedStatement.executeQuery();


            while (resultSet.next()){
                output.add(resultSet.getString("id"));
                output.add(resultSet.getString("contract_number"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;

    }

    public void alterQuantityByOne(){

        super.alterQuantityByOne();
        try {
            PreparedStatement preparedStatement = addQuantityByOneStatement;
            //System.out.println(preparedStatement.toString());
            preparedStatement.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void removeOrderBefore(String lodgementDate){
        super.removeOrderBefore(lodgementDate);

        try {
            PreparedStatement preparedStatement = removeOrderBeforeStatement;

            preparedStatement.setDate(1, Date.valueOf(lodgementDate));
            //System.out.println(preparedStatement.toString());
            preparedStatement.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getDirectByCon(String conNum){
        String output="" ;
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = getDirectByConStatement;

            preparedStatement.setString(1, conNum);
            //System.out.println(preparedStatement.toString());
            resultSet=preparedStatement.executeQuery();
            resultSet.next();
            output=(resultSet.getString("contract_number"));


        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    ArrayList<String> queryDirectSelfOrder(String name){
        super.queryDirectSelfOrder(name);
        ArrayList<String> output=new ArrayList<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = queryDirectSelfOrderStatement;

            preparedStatement.setString(1, name);

            //System.out.println(preparedStatement.toString());

            resultSet = preparedStatement.executeQuery();


            while (resultSet.next()){
                output.add(resultSet.getString("id"));
                output.add(resultSet.getString("contract_number"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;
    }
    ArrayList<String> querySalesmanSelfOrder(int number){
        super.querySalesmanSelfOrder(number);
        ArrayList<String> output=new ArrayList<>();
        ResultSet resultSet;
        try {
            PreparedStatement preparedStatement = querySalesmanSelfOrderStatement;

            preparedStatement.setInt(1, number);

            //System.out.println(preparedStatement.toString());

            resultSet = preparedStatement.executeQuery();


            while (resultSet.next()){
                output.add(resultSet.getString("id"));
                output.add(resultSet.getString("contract_number"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  output;
    }

}
