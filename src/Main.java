import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;

public class Main {

    static final String DB_LOCATION1 = "C:/Users/ezgi.taskomaz/Desktop/Thesis Files/DBS/worldcupdb";
    // Pokec Data Set with 400000K Nodes
    static final String DB_LOCATION2 = "C:/Users/ezgi.taskomaz/Desktop/Thesis Files/DBS/PokecDB";
    // Pokec Data Set with 200000K Nodes
    static final String DB_LOCATION3 = "C:/Users/ezgi.taskomaz/Desktop/Thesis Files/DBSOthers/PokecDB";

    static final String DB_LOCATION4 = "D:/neo4j-community-3.4.9/data/databases/graph.db";


    //static final String query1 = "(0,1,Country,NAMED_SQUAD,OUTGOING,Squad)(1,2,Squad,IN_SQUAD,INCOMING,Player)(2,3,Player,IN_SQUAD,OUTGOING,Squad)(3,4,Squad,NAMED_SQUAD,INCOMING,Country)";
    //static final String query2 = "(0,1,Time,PLAYED_AT_TIME,INCOMING,Match)(0,2,Time,PLAYED_AT_TIME,INCOMING,Match)(1,3,Match,PLAYED_IN,INCOMING,Country)(1,4,Match,PLAYED_IN,INCOMING,Country)(1,5,Match,CONTAINS_MATCH,INCOMING,WorldCup)(2,3,Match,PLAYED_IN,INCOMING,Country)(2,4,Match,PLAYED_IN,INCOMING,Country)(2,6,Match,CONTAINS_MATCH,INCOMING,WorldCup)";
    //static final String query3 = "(0,1,Player,STARTED,OUTGOING,Performance)(0,2,Player,STARTED,OUTGOING,Performance)(0,3,Player,STARTED,OUTGOING,Performance)(1,4,Performance,IN_MATCH,OUTGOING,Match)(2,5,Performance,IN_MATCH,OUTGOING,Match)(3,6,Performance,IN_MATCH,OUTGOING,Match)(4,7,Match,CONTAINS_MATCH,INCOMING,WorldCup)(5,8,Match,CONTAINS_MATCH,INCOMING,WorldCup)(6,9,Match,CONTAINS_MATCH,INCOMING,WorldCup)";
    //static final String query4 = "(0,1,Country,HOSTED_BY,INCOMING,WorldCup)(1,2,WorldCup,CONTAINS_MATCH,OUTGOING,Match)";
    //static final String query5 = "(0,1,Player,STARTED,OUTGOING,Performance)(0,2,Player,STARTED,OUTGOING,Performance)(0,3,Player,STARTED,OUTGOING,Performance)(1,4,Performance,IN_MATCH,OUTGOING,Match)(2,5,Performance,IN_MATCH,OUTGOING,Match)(3,6,Performance,IN_MATCH,OUTGOING,Match)(4,7,Match,CONTAINS_MATCH,INCOMING,WorldCup)(5,8,Match,CONTAINS_MATCH,INCOMING,WorldCup)(6,9,Match,CONTAINS_MATCH,INCOMING,WorldCup)";

    static String query1 = "(0,1,Country,NAMED_SQUAD,OUTGOING,Squad)";
    static String query2 = "(0,1,Country,NAMED_SQUAD,OUTGOING,Squad)(1,2,Squad,IN_SQUAD,INCOMING,Player)";
    static String query3 = "(0,1,Country,HOSTED_BY,INCOMING,WorldCup)";
    static String query4 = "(0,1,Country,HOSTED_BY,INCOMING,WorldCup)(1,2,WorldCup,CONTAINS_MATCH,OUTGOING,Match)";
    static String query5 = "(0,1,Country,NAMED_SQUAD,OUTGOING,Squad)(1,2,Squad,IN_SQUAD,INCOMING,Player)(2,3,Player,IN_SQUAD,OUTGOING,Squad)(3,4,Squad,NAMED_SQUAD,INCOMING,Country)";
    static String query6 = "(0,1,Time,PLAYED_AT_TIME,INCOMING,Match)(0,2,Time,PLAYED_AT_TIME,INCOMING,Match)(1,3,Match,PLAYED_IN,INCOMING,Country)(1,4,Match,PLAYED_IN,INCOMING,Country)(1,5,Match,CONTAINS_MATCH,INCOMING,WorldCup)(2,3,Match,PLAYED_IN,INCOMING,Country)(2,4,Match,PLAYED_IN,INCOMING,Country)(2,6,Match,CONTAINS_MATCH,INCOMING,WorldCup)";
    static String query7 = "(0,1,Player,STARTED,OUTGOING,Performance)(0,2,Player,STARTED,OUTGOING,Performance)(0,3,Player,STARTED,OUTGOING,Performance)(1,4,Performance,IN_MATCH,OUTGOING,Match)(2,5,Performance,IN_MATCH,OUTGOING,Match)(3,6,Performance,IN_MATCH,OUTGOING,Match)(4,7,Match,CONTAINS_MATCH,INCOMING,WorldCup)(5,8,Match,CONTAINS_MATCH,INCOMING,WorldCup)(6,9,Match,CONTAINS_MATCH,INCOMING,WorldCup)";

    static String query8 = "(0,1,User&UserId=2,friends,OUTGOING,User&UserId=22)";
    static String query9 = "(0,1,User,worksIn,OUTGOING,Occupation&EducationLevel=stredoskolske)(0,2,User,hasProfile,OUTGOING,profile&Age=17)";
    static String query10 = "(0,1,User,activeDoing,OUTGOING,Sports&name=basketbal)(0,2,User,passiveDoing,OUTGOING,Sports&name=skateboarding)";
    static String query11 = "(0,1,User,worksIn,OUTGOING,Occupation&EducationLevel=vysokoskolske)(0,2,User,talks,OUTGOING,Language&name=english)";
    static String query12 = "(0,1,User,worksIn,OUTGOING,Occupation)(0,2,User,friends,OUTGOING,User)(2,3,User,activeDoing,OUTGOING,Sports)";
    static String query13 = "(0,1,User,talks,OUTGOING,Language)(0,2,friends,OUTGOING,User)(2,3,talks,OUTGOING,Language)";
    static String query14 = "(0,1,User,talks,OUTGOING,Language)(0,2,friends,OUTGOING,User)(2,3,talks,OUTGOING,Language)";

    static String query15 = "(0,1,User,worksIn,OUTGOING,Occupation)";
    static String query16 = "(0,1,Country&name=Sweden,NAMED_SQUAD,OUTGOING,Squad)";
    static String query17 = "(0,1,User,worksIn,OUTGOING,Occupation&EducationLevel=stredoskolske)";
    static String query18 = "(0,1,User,worksIn,OUTGOING,Occupation)(0,2,User,hasProfile,OUTGOING,profile&Age=17)";

    static String populationQuery1 = "(0,1,KİŞİ,EŞİ,OUTGOING,KİŞİ)(0,2,KİŞİ,ESKİ EŞİ,OUTGOING,KİŞİ)(0,3,KİŞİ,BABASI,INCOMING,KİŞİ)(0,4,KİŞİ,BABASI,INCOMING,KİŞİ)(0,5,KİŞİ,YERLEŞİM_YERİ,OUTGOING,BAĞIMSIZ_BÖLÜM)(1,3,KİŞİ,ANNESİ,INCOMING,KİŞİ)(1,5,KİŞİ,YERLEŞİM_YERİ,OUTGOING,BAĞIMSIZ_BÖLÜM)(2,4,KİŞİ,ANNESİ,INCOMING,KİŞİ)(2,6,KİŞİ,YERLEŞİM_YERİ,OUTGOING,BAĞIMSIZ_BÖLÜM)(3,5,KİŞİ,YERLEŞİM_YERİ,OUTGOING,BAĞIMSIZ_BÖLÜM)(4,5,KİŞİ,YERLEŞİM_YERİ,OUTGOING,BAĞIMSIZ_BÖLÜM)";

    static String worldcupQuery1 = "(0,1,Country,NAMED_SQUAD,OUTGOING,Squad)(1,2,Squad,IN_SQUAD,INCOMING,Player)(2,3,Player,IN_SQUAD,OUTGOING,Squad)(3,4,Squad,NAMED_SQUAD,INCOMING,Country)";
    static String worldcupQuery2 = "(0,1,Player,STARTED,OUTGOING,Performance)(0,2,Player,SUBSTITUTE,OUTGOING,Performance)(1,3,Performance,IN_MATCH,OUTGOING,Match)(2,3,Performance,IN_MATCH,OUTGOING,Match)";
    static String worldcupQuery3 = "(0,1,Time,PLAYED_AT_TIME,INCOMING,Match)(0,2,Time,PLAYED_AT_TIME,INCOMING,Match)(1,3,Match,PLAYED_IN,INCOMING,Country)(1,4,Match,PLAYED_IN,INCOMING,Country)(1,5,Match,CONTAINS_MATCH,INCOMING,WorldCup)(2,3,Match,PLAYED_IN,INCOMING,Country)(2,4,Match,PLAYED_IN,INCOMING,Country)(2,6,Match,CONTAINS_MATCH,INCOMING,WorldCup)";
    static String worldcupQuery4 = "(0,1,WorldCup,CONTAINS_MATCH,OUTGOING,Match)(0,2,WorldCup,CONTAINS_MATCH,OUTGOING,Match)(1,3,Match,IN_MATCH,INCOMING,Performance)(1,4,Match,HOME_TEAM,OUTGOING,Country)(1,5,Match,AWAY_TEAM,OUTGOING,Country)(2,4,Match,AWAY_TEAM,OUTGOING,Country)(2,5,Match,HOME_TEAM,OUTGOING,Country)(2,6,Match,IN_MATCH,INCOMING,Performance)(3,7,Performance,SCORED_GOAL,OUTGOING,Goal)(3,8,Performance,STARTED,INCOMING,Player)(6,8,Performance,STARTED,INCOMING,Player)(6,9,Performance,SCORED_GOAL,OUTGOING,Goal)";
    static String worldcupQuery5 = "(0,1,Player,STARTED,OUTGOING,Performance)(0,2,Player,STARTED,OUTGOING,Performance)(0,3,Player,STARTED,OUTGOING,Performance)(1,4,Performance,IN_MATCH,OUTGOING,Match)(2,5,Performance,IN_MATCH,OUTGOING,Match)(3,6,Performance,IN_MATCH,OUTGOING,Match)(4,7,Match,CONTAINS_MATCH,INCOMING,WorldCup)(5,8,Match,CONTAINS_MATCH,INCOMING,WorldCup)(6,9,Match,CONTAINS_MATCH,INCOMING,WorldCup)";


    public static void main(String[] args) {
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService db = dbFactory.newEmbeddedDatabase(new File(DB_LOCATION1));

//        GraphDatabaseSettings.BoltConnector bolt = GraphDatabaseSettings.boltConnector( "0" );
//        GraphDatabaseService db = new GraphDatabaseFactory()
//                .newEmbeddedDatabaseBuilder(new File( DB_LOCATION3) )
//                .setConfig( bolt.type, "BOLT" )
//                .setConfig( bolt.enabled, "true" )
//                .setConfig( bolt.address, "localhost:7687" )
//                .newGraphDatabase();


        String query = query4;

        //runBBGraphQuery(db, query);
        //runTurboIsoQuery(db, query);
        //runGraphqlQuery(db, query);
        //runDualIsoQuery(db, query);
        //runVF3Query(db, query);
        runBBPlusQuery(db, query);
        //runBBMstQuery(db, query);
    }

    public static void runBBMstQuery(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            MstGraph mstGraph = new MstGraph(db);
            mstGraph.executeQuery(query);
            tx.success();
        } catch (Exception e){
            throw new IllegalStateException("Null Property: ", e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction 1!");
        }
    }

    public static void runBBPlusQuery(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            BBPlus bbPlus = new BBPlus(db);
            bbPlus.executeQuery(query);
            tx.success();
        } catch (Exception e){
            throw new IllegalStateException("Null Property: ", e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction 1!");
        }
    }

    public static void runBBGraphQuery(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            BasicMethod basicMethod = new BasicMethod(db);
            basicMethod.executeQuery(query);
            tx.success();
        } catch (Exception e){
            System.out.println(e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction!");
        }
    }

    public static void runMstGraphQuery(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            MstGraph mstGraph = new MstGraph(db);
            mstGraph.executeQuery(query);
            tx.success();
        }
        catch (Exception e){
            System.out.println(e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction 1!");
        }
    }

    public static void runTurboIsoQuery(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            TurboMethod turboMethod = new TurboMethod(db);
            turboMethod.executeQuery(query);
            tx.success();
        }
        catch (Exception e){
            System.out.println(e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction 1!");
        }
    }

    public static void runDualIsoQuery(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            DualIso dualIso = new DualIso(db);
            dualIso.executeQuery(query);

            tx.success();
        }
        catch (Exception e){
            throw new IllegalStateException("Null Property: ", e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction 1!");
        }
    }


    public static void runVF3Query(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            VF3 vf3 = new VF3(db);
            vf3.executeQuery(query);

            tx.success();
        }
        catch (Exception e){
            throw new IllegalStateException("Null Property: ", e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction 1!");
        }
    }

    public static void runGraphqlQuery(GraphDatabaseService db, String query) {
        Transaction tx = db.beginTx();
        try{
            GraphQL graphQL = new GraphQL(db);
            graphQL.executeQuery(query);
            tx.success();
        }
        catch (Exception e){
            System.out.println(e);
        }
        finally{
            tx.close();
            db.shutdown();
            System.out.println("DB is closed after transaction 1!");
        }
    }

}
