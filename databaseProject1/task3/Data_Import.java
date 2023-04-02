import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;
import java.sql.*;

class locationDI{
    String country;
    String city;
    public locationDI(){
        country=null;city=null;
    }
    public locationDI(String city,String country){
        this.country=country;
        this.city=city;
    }
}

public class Data_Import {
    private static final int    BATCH_SIZE = 500;//批量处理数目

    private static Connection          con = null;
    private static PreparedStatement  productStmt = null;
    private static PreparedStatement  contractStmt = null;
    private static PreparedStatement  orderStmt = null;
    private static PreparedStatement  supplyCenterStmt = null;
    private static PreparedStatement  salesmanStmt = null;
    private static PreparedStatement  modelStmt = null;
    private static PreparedStatement  locationStmt = null;
    private static PreparedStatement  clientStmt = null;
    private static boolean         verbose = false;

    private static void openDB(String host, String dbname, String user, String pwd){
        try {
            Class.forName("org.postgresql.Driver");
        } catch(Exception e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + host + "/" + dbname;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pwd);
        try {
            con = DriverManager.getConnection(url, props);
            if (verbose) {
                System.out.println("Successfully connected to the database " + dbname + " as " + user);
            }
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        try {
            productStmt = con.prepareStatement("INSERT INTO product(code,product_name)" + " values(?,?)"+
                    "ON CONFLICT (code) DO NOTHING;");//insert模板
            salesmanStmt=con.prepareStatement("INSERT INTO salesman(number,name,gender,age,phone_number)" +
                    " values(to_number(?,'999999999'),?,?,to_number(?,'999999999'),to_number(?,'999999999'))"+
                    "ON CONFLICT (number) DO NOTHING;");
            locationStmt=con.prepareStatement("INSERT INTO location(city,country)"+ " values(NULLIF(?,'NULL'),?)"+
                    "ON CONFLICT(city,country) DO NOTHING;");////????
            supplyCenterStmt=con.prepareStatement("INSERT INTO supply_center(center_name,director_name)" + " values(?,?)"+
                    "ON CONFLICT (center_name,director_name) DO NOTHING;");
            clientStmt=con.prepareStatement("INSERT INTO client_enterprise(name,industry,location_id,supply_center)" +
                    " values(?,?,to_number(?,'999999999'),?)"+ "ON CONFLICT (name) DO NOTHING;");///???
            contractStmt= con.prepareStatement("INSERT INTO contract(number,contract_date,client_enterprise)" +
                    " values(?,to_date( ? ,'yyyy-mm-dd'),?)"+ "ON CONFLICT (number) DO NOTHING;");
            modelStmt=con.prepareStatement("INSERT INTO model(model_name,unit_price,product_code)"+ " values(?,to_number(?,'999999999'),?)"+
                    "ON CONFLICT (model_name) DO NOTHING;");
            orderStmt=con.prepareStatement("INSERT INTO contract_order(id,contract_number,estimated_delivery_date,lodgement_date," +
                    "quantity,model,salesman_number)"
                    +" values(?,?,to_date( ? ,'yyyy-mm-dd'),to_date( NULLIF(?,'') ,'yyyy-mm-dd')," +
                    "to_number(?,'999999999'),?,to_number(?,'999999999'))");
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private static void closeDB() {
        if (con != null) {
            try {
                if (productStmt != null) {
                    productStmt.close();
                }
                if (salesmanStmt != null) {
                    salesmanStmt.close();
                }
                if (locationStmt != null) {
                    locationStmt.close();
                }
                if (supplyCenterStmt!= null) {
                    supplyCenterStmt.close();
                }
                if (clientStmt!= null) {
                    clientStmt.close();
                }
                if (contractStmt != null) {
                    contractStmt.close();
                }
                if (modelStmt != null) {
                    modelStmt.close();
                }
                if (orderStmt != null) {
                    orderStmt.close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }

    private static void loadProductData(String code, String product_name)
            throws SQLException {
        if (con != null) {
            productStmt.setString(1, code);
            productStmt.setString(2, product_name);
            productStmt.addBatch();
        }
    }

    private static void loadContractData(String number,String date,String client)
            throws SQLException {
        if (con != null) {
            contractStmt.setString( 1, number);
            contractStmt.setString( 2, date);
            contractStmt.setString( 3, client);
            contractStmt.addBatch();
        }
    }

    private static void loadOrderData(int id, String contract_num,String edate,String ldate,
                                      String quantity,String product,String salenum)
            throws SQLException {
        if (con != null) {
            orderStmt.setInt(1,id);
            orderStmt.setString(2, contract_num);
            orderStmt.setString(3,edate);
            orderStmt.setString(4,ldate);
            orderStmt.setString(5,quantity);
            orderStmt.setString(6,product);
            orderStmt.setString(7,salenum);
            orderStmt.addBatch();
        }
    }

    private static void loadSupplyCenterData(String name, String director_name)
            throws SQLException {
        if (con != null) {
            supplyCenterStmt.setString(1, name);
            supplyCenterStmt.setString(2, director_name);
            supplyCenterStmt.addBatch();
        }
    }

    private static void loadSalesmanData(String number, String name,String gender,String age,String phone)
            throws SQLException {
        if (con != null) {
            salesmanStmt.setString(1, number);
            salesmanStmt.setString(2, name);
            salesmanStmt.setString(3, gender);
            salesmanStmt.setString(4, age);
            salesmanStmt.setString(5, phone);
            salesmanStmt.addBatch();
        }
    }

    private static void loadModuleData(String name,String price, String code)
            throws SQLException {
        if (con != null) {
            modelStmt.setString(1, name);
            modelStmt.setString(2, price);
            modelStmt.setString(3, code);
            modelStmt.addBatch();
        }
    }

    private static void loadLocationData(String city, String country)
            throws SQLException {
        if (con != null) {
            locationStmt.setString(1, city);
            locationStmt.setString(2, country);
            locationStmt.addBatch();
        }
    }

    private static void loadClientData(String name, String industry,String locationId,String supplyCenter)
            throws SQLException {
        if (con != null) {
            clientStmt.setString(1, name);
            clientStmt.setString(2, industry);
            clientStmt.setString(3,locationId);
            clientStmt.setString(4,supplyCenter);
            clientStmt.addBatch();
        }
    }

    public static void main(String[] args) {
        String  fileName = null;


        switch (args.length) {
            case 1:
                fileName = args[0];
                break;
            case 2:
                if ("-v".equals(args[0])) {
                    verbose = true;
                } else {
                    System.err.println("Usage: java [-v] GoodLoader filename");
                    System.exit(1);
                }
                fileName = args[1];
                break;
            default:
                System.err.println("Usage: java [-v] GoodLoader filename");
                System.exit(1);
        }

        //默认参数
        Properties defProp = new Properties();
        defProp.put("host", "localhost");
        defProp.put("user", "checker");
        defProp.put("password", "123456");
        defProp.put("database", "sustc");
        Properties prop = new Properties(defProp);

        try (BufferedReader infile = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8))){
            long     start;
            long     end;
            int      cnt = 0;

            String   line;
            String[] parts;
            String   contract_name;
            String   client_enterprise;
            String   supply_center;
            String   country;
            ArrayList<locationDI> location=new ArrayList<>();
            location.add(new locationDI());
            String   city;
            String   industry;
            String   product_code;
            String   product_name;
            String   product_model;
            String   unit_price;
            String   quantity;
            String   contract_date;
            String   estimated_delivery_date;
            String   lodgement_date;
            String   director;
            String   salesman;
            String   salesman_number;
            String   gender;
            String   age;
            String   phone;

            // Empty table
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            Statement stmt0;
            if (con != null) {
                stmt0 = con.createStatement();
                stmt0.execute("TRUNCATE TABLE product CASCADE;");
                System.out.println("Empty the product table successfully");
                stmt0.execute("TRUNCATE TABLE salesman CASCADE;");
                System.out.println("Empty the salesman table successfully");
                stmt0.execute("TRUNCATE TABLE location RESTART IDENTITY CASCADE;");
                System.out.println("Empty the location table successfully");
                stmt0.execute("TRUNCATE TABLE supply_center CASCADE;");
                System.out.println("Empty the supply center table successfully");
                stmt0.execute("TRUNCATE TABLE client_enterprise CASCADE;");
                System.out.println("Empty the client enterprise table successfully");
                stmt0.execute("TRUNCATE TABLE contract CASCADE;");
                System.out.println("Empty the contract table successfully");
                stmt0.execute("TRUNCATE TABLE model CASCADE;");
                System.out.println("Empty the model table successfully");
                stmt0.execute("TRUNCATE TABLE contract_order CASCADE;");
                System.out.println("Empty the order table successfully");
                System.out.println();
                con.commit();
                stmt0.close();
            }

            closeDB();

            //打开数据库
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                   prop.getProperty("user"), prop.getProperty("password"));
            /////contract
            String contractLastname=null;
            int contractID=0;
            infile.readLine();//第一行
            //计时开始
            start = System.currentTimeMillis();
            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts.length > 1) {
                    contract_name= parts[0].replace(",", "");
                    client_enterprise=parts[1].replace(",", "");
                    supply_center= parts[2].replace(",", "");
                    country      = parts[3].replace(",", "");
                    city         = parts[4].replace(",", "");
                    industry     = parts[5].replace(",", "");
                    product_code = parts[6].replace(",", "");
                    product_name = parts[7].replace(",", "");
                    product_model= parts[8].replace(",", "");
                    unit_price   = parts[9].replace(",", "");
                    quantity     = parts[10].replace(",", "");
                    contract_date= parts[11].replace(",", "");
                    estimated_delivery_date=parts[12].replace(",", "");
                    lodgement_date=parts[13].replace(",", "");
                    director     = parts[14].replace(",", "");
                    salesman     = parts[15].replace(",", "");
                    salesman_number  = parts[16].replace(",", "");
                    gender       = parts[17].replace(",", "");
                    age          = parts[18].replace(",", "");
                    phone        = parts[19].replace(",", "");

                    //LoadData
                    loadProductData(product_code, product_name);
                    loadSalesmanData(salesman_number,salesman,gender,age,phone);
                    ///loadLocation
                    boolean locationNoRepeat=true;
                    int id=0;
                    for (int i=0;i<location.size();i++) {
                        if (city.equals(location.get(i).city) && country.equals(location.get(i).country)) {
                            locationNoRepeat = false;
                            id=i;
                            break;
                        }
                    }
                    if (locationNoRepeat){
                        loadLocationData(city,country);
                        location.add(new locationDI(city,country));
                        loadClientData(client_enterprise,industry,String.valueOf(location.size()-1),supply_center);////id折腾？
                    }
                    if (!locationNoRepeat){
                        loadClientData(client_enterprise,industry,String.valueOf(id),supply_center);////id折腾？
                    }/////能否再优化 少几步？
                    ///
                    loadSupplyCenterData(supply_center,director);
                    loadContractData(contract_name,contract_date,client_enterprise);
                    loadModuleData(product_model,unit_price,product_code);
                    ///

                    if (!contract_name.equals(contractLastname)){
                        contractLastname=contract_name;
                        contractID=1;
                    }else {
                       contractID++;
                    }
                    loadOrderData(contractID,contract_name,estimated_delivery_date,lodgement_date,
                            quantity,product_model,salesman_number);
                    ///

                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        productStmt.executeBatch();
                        productStmt.clearBatch();
                        salesmanStmt.executeBatch();
                        salesmanStmt.clearBatch();
                        locationStmt.executeBatch();
                        locationStmt.clearBatch();
                        supplyCenterStmt.executeBatch();
                        supplyCenterStmt.clearBatch();
                        clientStmt.executeBatch();
                        clientStmt.clearBatch();
                        contractStmt.executeBatch();
                        contractStmt.clearBatch();
                        modelStmt.executeBatch();
                        modelStmt.clearBatch();
                        orderStmt.executeBatch();
                        orderStmt.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                productStmt.executeBatch();
                salesmanStmt.executeBatch();
                locationStmt.executeBatch();
                supplyCenterStmt.executeBatch();
                clientStmt.executeBatch();
                contractStmt.executeBatch();
                modelStmt.executeBatch();
                orderStmt.executeBatch();
            }
            con.commit();
            productStmt.close();
            salesmanStmt.close();
            locationStmt.close();
            supplyCenterStmt.close();
            clientStmt.close();
            contractStmt.close();
            modelStmt.close();
            orderStmt.close();

            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded");
            System.out.println("Loading total time : " + (double)(end - start)/1000 + " s");
            System.out.println("Loading speed : " + (cnt * 1000L )/(end - start) + " records/s");
        }
        catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                productStmt.close();
                salesmanStmt.close();
                locationStmt.close();
                supplyCenterStmt.close();
                clientStmt.close();
                contractStmt.close();
                modelStmt.close();
                orderStmt.close();
            } catch (Exception e2) {
                //
            }
            closeDB();
            System.exit(1);
        }
        catch (IOException e){
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                productStmt.close();
                salesmanStmt.close();
                locationStmt.close();
                supplyCenterStmt.close();
                clientStmt.close();
                contractStmt.close();
                modelStmt.close();
                orderStmt.close();
            } catch (Exception e2) {
                //
            }
            closeDB();
            System.exit(1);
        }
    }
}
