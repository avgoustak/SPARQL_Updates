/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package NewVersionProvenance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Argyro Avgoustaki
 * @email argiro@ics.forth.gr
 */
public class VirtuosoOperations {
   public static Connection conn;
   public static Statement statement;
    
public VirtuosoOperations(String usr, String pwd) throws ClassNotFoundException, SQLException {
        Class.forName("virtuoso.jdbc4.Driver");
        conn = DriverManager.getConnection("jdbc:virtuoso://localhost:1111/" + "/charset=UTF-8/log_enable=2", usr, pwd);
        statement = conn.createStatement();
}

public Statement getStatement() {
        return this.statement;
}

public Connection getConnection() {
        return this.conn;
}

/* This function closes the connection with the Virtuoso database
 */    
public void terminate() {
        try {
            statement.close();
            conn.close();
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured during the close of statement and connection.");
        }
    }

/**
 *
 * This function executes a SPARQL INSERT update using as Dataset the current instance of Virtuoso
 * @param query A SPARQL INSERT update
 * @param st
 */
public static void executeSQLUpdate(String query) {
   try {
       //long startTime = System.currentTimeMillis();
       statement.executeQuery("log_enable(3,1)");
       statement.executeUpdate(query);
       long endTime = System.currentTimeMillis();
       //System.out.println("Update " +query+ "committed in " + (endTime - startTime) + "ms");

        } catch (SQLException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured during the update query.");
        }
}

/**
  * Executes an SQL query of the form "Select .... From ....."
  * @param query An SQL query
  * @return the results of the select statement
  */
public static ResultSet executeSQLQuery(String query) {
    try {
        ResultSet result;
       // long startTime = System.currentTimeMillis();

        statement.setFetchSize(10000);
        result = statement.executeQuery(query);
        
      //  long endTime = System.currentTimeMillis();
       // System.out.println("Query "+query+ " committed in " + (endTime - startTime) + " ms");
        return result;
    } catch (SQLException ex) {
        System.out.println("Exception: "+ex.getMessage()+" occured during the execution of a SELECT query!");
       // System.out.println("Exception: "+ex.getErrorCode());
       // System.out.println("Exception: "+ex.getCause());
       // System.out.println("Exception: "+ex.getNextException());
        return null;
    }

}

/**
 * This function loads an RDF file that contains quads in N-Quad form and inserts the
 * corresponding quads into Virtuoso,
 * computing at the same time their provenance. We assume that the input file does not contain any provenance
 * information.
 * @param filename the name of the file. Note that the extension of this filename is always .nq
 * and can not be specified or altered by the user
 * @param provInfo 0 if the file does not contain provenance information or 1 otherwise //to be done
 */
public static void loadRDFFileToVirtuoso(String filename, int provInfo) throws SQLException{
    try{
        String s = null,p = null,o = null,g = null, query, provenance = null;
        int i=0, QuadID = 0, CPE;
        Matcher m;
        ResultSet result;
        File inputFile =  new File(filename);
       // System.out.println("file"+ inputFile.getAbsolutePath());

        //Construct the file reader
        FileReader fileReader = new FileReader(inputFile);

        //Construct the buffer reader from file reader
        BufferedReader reader = new BufferedReader(fileReader);
        String currentLine;
        while((currentLine = reader.readLine()) != null){
            i = 0;
            m = Pattern.compile("(<[^<>]*>|\"[^\"]*\")").matcher(currentLine);
            while(m.find()){/*Parsing the line*/
               // System.out.println("find: "+m.group(0));
                switch(i){
                    case 0:
                        s = m.group(0);
                       
                        break;
                    case 1:
                        p = m.group(0);
                        
                        break;
                    case 2:
                        
                        o = m.group(0);
                        break;
                    case 3:
                      
                        g = m.group(0);
                        break;
                    case 4:
                        provenance = m.group(0);
                        break;
                    default:
                        break;
                }
                i++;

            }
            System.out.println("s: "+s+" p: "+p+" o: "+o+" g: "+g);
             query = "select quad_ID from DB.DBA.MYQUADS where subject = '"+s+"' and predicate = '"+p+"' and object = '"+o+"' and graph = '"+g+"'";
             result = executeSQLQuery(query);

            

            try {

                if(!result.next()){//The quadruple is inserted for first time in the instance
                   query = "insert into DB.DBA.MYQUADS (subject,predicate,object,graph) values ('"+s+"','"+p+"','"+o+"','"+g+"')";
                   executeSQLUpdate(query);
                   QuadID = getQuadID(s,p,o,g);
                   query = "insert into DB.DBA.MyProvenance (quad_ID,cpe,pe,prov_s,prov_p,prov_o) values"
                            + "('"+QuadID+"','1','1','Unknown','Unknown','Unknown')";
                   executeSQLUpdate(query);

                }else //The quadruple is already in the system.In this case we only insert the provenance (Unknown,Unknown,Unknown)
                {

                    QuadID = result.getInt(1);
                    //System.out.println("QuadID: "+QuadID);
                    CPE = getMaxCPENumber(QuadID)+1;
                    //System.out.println("LastCPE: "+ CPE);
                    query = "insert into DB.DBA.MyProvenance (quad_ID,cpe,pe,prov_s,prov_p,prov_o) values"
                            + "('"+QuadID+"','"+CPE+"','1','Unknown','Unknown','Unknown')";
                   statement.executeUpdate(query);

                }
            } catch (SQLException ex) {
                System.out.println("Exception "+ ex.getMessage()+"occured at loadRDFFile function!");
            }
       }
        //close readers and writers
        reader.close();
        fileReader.close();
     } catch(IOException e){
        System.out.println("Exception: "+e.getMessage()+" in function loadRDFFile!");
 }
}

/**
 * This function searches if the input quadruple already exists in the system
 * @param s the subject of the quad
 * @param p the presicate of the quad
 * @param o the object of the quad
 * @param g the named graph of the quad
 * @return true if the quad already exists in Virtuoso,false if not
 */
public static boolean searchQuad(String s, String p, String o, String g) throws SQLException{
   return statement.execute("select quad_ID from DB.DBA.MYQUADS where subject = '"+s+"' and predicate = '"+p+"' and object = '"+o+"' and graph = '"+g+"'");
}

/**
 * This function finds out if a quad already exists in Virtuoso
 * @param s the subject of the quad
 * @param p the predicate of the quad
 * @param o the object of the quad
 * @param g the named graph of the quad
 * @return 0 if the quad does not exist in Virtuoso, or the number of the quad_id if it is already exists
 */
public static int getQuadID (String s, String p, String o, String g){
    ResultSet id;
    int QuadID = 0;
    try {
            id = executeSQLQuery("select quad_id from DB.DBA.MYQUADS where subject = '" + s + "' and predicate = '" + p + "' and object = '" + o + "' and graph = '" + g + "'");

    if (!id.next()){ //The result set contains no data,i.e., no quad exists with these components
        return 0;
    }
    QuadID = id.getInt(1);
    } catch (SQLException ex) {
            System.out.println("Exception: "+ ex.getMessage()+" occured in function getQuadID!");
        }
    return QuadID;
}

/**
 * This function finds out if a quad already exists in Virtuoso
 * @param qp a quad(s,p,o,g)
 * @return 0 if the quad does not exist in Virtuoso, or the number of the quad_id if it is already exists
 */
public int getQuadID (QuadPattern qp){
    ResultSet id;
    int QuadID = 0;
    try {
            id = statement.executeQuery("select quad_id from DB.DBA.MYQUADS where subject = '" + qp.getSubject() + "' and predicate = '" + qp.getPredicate() + "' and object = '" + qp.getObject() + "' and graph = '" + qp.getGraph() + "'");

    if (!id.next()){ //The result set contains no data,i.e., no quad exists with these components
        return 0;
    }
    QuadID = id.getInt(1);
     } catch (SQLException ex) {
            System.out.println("Exception: "+ ex.getMessage()+" occured in function getQuadID!");
     }
    return QuadID;
}

/**
 * Finds the last registered cpe number in the table MyProvenance for the specific QuadPattern
 * @param qp the element for which we want to find the last cpe
 * @return 
 */
public int getLastCPENumber (QuadPattern qp){
    ResultSet id;
    int lastCPE = 0;
    try {
        id = statement.executeQuery("select max(cpe) from DB.DBA.MyProvenance where subject = '"+qp.getSubject()+"' and predicate = '"
                +qp.getPredicate()+"' and object = '"+qp.getObject()+"' and graph = '"+qp.getGraph()+"'");

    //printResults(id);
     if (!id.next()){ //The result set contains no data,i.e., no cpe exists for this quad_id
        return 0;
    }
    lastCPE = id.getInt(1);
    } catch (SQLException ex) {
            System.out.println("Exception: "+ex.getMessage()+" in function getLastCPENumber!");
        }
    System.out.println("lastCPE: "+lastCPE);
    return lastCPE;
}

/**
 * This function searches for the max cpe number of the input quad_ID in the table MyProvenance of Virtuoso
 * @param quad_ID an integer that represents a quad_ID in the table MyProvenance of Virtuoso
 * @return an integer that represents the last cpe number for the specidic quadruple
 */
public static int getMaxCPENumber(int quad_ID) {
    
    ResultSet res;
    int lastCPE = 0;
    try {
       res = statement.executeQuery("select max(cpe) from DB.DBA.MyProvenance where quad_ID = "+ quad_ID);
       res.next();
       lastCPE = res.getInt(1);
     } catch (SQLException ex) {
            System.out.println("Exception: "+ex.getMessage()+" in function getLastCpeNumber!");
     }
    return lastCPE;
}

public static int getLastPENumber(int quad_ID, int cpe) {

    ResultSet res;
    int lastPE = 0;
    try {
       res = statement.executeQuery("select max(pe) from DB.DBA.MyProvenance where quad_ID = "+ quad_ID+" and cpe = "+cpe);
       res.next();
       lastPE = res.getInt(1);
     } catch (SQLException ex) {
            System.out.println("Exception: "+ex.getMessage()+" in function getLastCpeNumber!");
     }
    return lastPE;
}

/**
 * This function finds the quadruple with the specific quadruple ID int Virtuoso
 * @param quad_ID a positive integer
 * @return the quadruple in the form of a </code>QuadPattern</code>
 */
public static QuadPattern getQuadfromID(int quad_ID){
    ResultSet res;
    QuadPattern quad = null ;
    try {
            res = statement.executeQuery("select subject, predicate, object, graph from DB.DBA.MYQUADS where quad_ID = '" + quad_ID + "'");
            if( res != null){
            res.next();
            quad = new QuadPattern();
            quad.setSubject(res.getString(1));
            quad.setPredicate(res.getString(2));
            quad.setObject(res.getString(3));
            quad.setGraph(res.getString(4));
            
            }else{
                System.out.println("The input quadruple ID does not exist!");
                return null;
            }
        } catch (SQLException ex) {
            System.out.println("Exception: "+ ex.getMessage()+" in function getQuadfromID!");

        }
    return quad;
}

/**
 * The function inserts the quadruples existing in an RDF file into the Virtuoso RDF_QUADS table
 * @params the filename of the RDF file
**/
public void importRDFFile(String filename) throws SQLException {
String query = "RDF_LOAD_RDFXML_MT(file_to_string_output('" + filename + "'), '', '" + "DB.DBA.RDF_QUADS" + "')";
statement.executeUpdate(query);
}

}
