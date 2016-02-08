/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package NewVersionProvenance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

/**
 *
 * @author Argyro Avgoustaki
 * @email argiro@ics.forth.gr
 */
public class Utilities{

//Maximum number of Unions
private static final int N = 1009;

/*
 * The function takes as input an INSERT Update of the form INSERT {qp_ins} WHERE{gp}
 * @return the input INSERT Update
 */
public static String inputUpdate(){
   Scanner input = new Scanner(System.in);
   System.out.println("Please enter a SPARQL INSERT Update: ");
   return input.nextLine();
}


/**
 * This function checks if the paramater is a literal, i.e. "..."
 * @param token a String
 * @return true if token is a literal, otherwise false 
 */
public static boolean isLiteral(String token){
    if(token.startsWith("\"")&& token.endsWith("\"")){
       return true;
    }else
        return false;   
}

/**
 * This function checks if the paramater is an IRI, i.e. <...>
 * @param token a String
 * @return true if token is a literal, otherwise false
 */
public static boolean isIri(String token){
    if(token.startsWith("<") && token.endsWith(">")){
       return true;
    }else
        return false;
}

/**
 * This function checks if the paramater is a variable, i.e. ?variable
 * @param token a String
 * @return true if token is a variable, otherwise false
 */
public static boolean isVariable(String token){
    if(token.startsWith("?")){
       return true;
    }else
        return false;
}

/** This function removes the triples/quads of a file that contains a blank node
 * @param filename : the file name that contains the triple that will be inserted in Virtuoso
 * @param removeText : the blank node symbol :_
 * @return returns the name of the newly created file else null if no new file is created
 * @throws IOException
 */
public static String removeBlankNodes(String filename, String removeText) throws IOException {
    File inputFile =  new File(filename);
    File outputFile = new File("NoBN_"+filename);

    //Construct the file reader
    FileReader fileReader = new FileReader(inputFile);
    //Construct the file writer
    FileWriter fileWriter = new FileWriter(outputFile);

    //Construct the buffer reader from file reader
    BufferedReader reader = new BufferedReader(fileReader);
    PrintWriter writer = new PrintWriter(fileWriter);
    String currentLine;

    try{

        while((currentLine = reader.readLine()) != null){
            if (!currentLine.trim().contains(removeText)){
                writer.println(currentLine);
                writer.flush();
            }
        }
        //close readers and writers
        reader.close();
        writer.close();
        fileReader.close();
        fileWriter.close();
        } catch(IOException e){
        System.out.println("Exception: "+e.getMessage()+" in function removeBlankNodes!");
    }
    return outputFile.getName();
}

/** This function prints the results of a select query.
 * @param result A Resultset that contains the results of a select query
 */
public static void printResultSet(ResultSet result){

    try {
        ResultSetMetaData meta = result.getMetaData();
        int count = meta.getColumnCount();
        while (result.next()) {
                for (int c = 1; c <= count; c++) {
                    System.out.print(result.getString(c));

                }System.out.println("");
            }
    } catch (SQLException ex) {
       System.out.println("Exception " + ex.getMessage() + "occured during printing the results of a query.");
    }
}


/**
 * Prints the contents of a LinkedHashSet that consists of </code>QuadPatternID</code>
 * @param set a LinkedHashSet of quad pattern identifiers
 */
public static void printSetID(Set<QuadPatternID> set){
    if(set == null){
        System.out.println("Wrong parameter in function printLinkedHashSet");

        return;
    }

    Iterator itr = set.iterator();
    QuadPatternID qp_id;

    while(itr.hasNext()){
        qp_id =  (QuadPatternID)itr.next();
        System.out.print(" qp^"+qp_id.getUnionNo()+"_"+ qp_id.getJoinNo());
    }

}

/**
 * Prints the contents of a set that consists of </code>QuadPatternPosition</code>
 * @param set a LinkedHashset of quad pattern positions
 */
public static void printSetPos(Set<QuadPatternPosition> set){
    if(set == null){
        System.out.println("Wrong parameter in function printLinkedHashSet");
        return;
    }


    Iterator itr = set.iterator();
    QuadPatternPosition qp_pos;

    while(itr.hasNext()){
        qp_pos =  (QuadPatternPosition)itr.next();
        System.out.print(" qp^"+qp_pos.getQuadPatternID().getUnionNo()+"_"+
             qp_pos.getQuadPatternID().getJoinNo()+"."+
             qp_pos.getPosition()+" ");
    }

}

/**
 * Prints the contents of a set that consists of </code>QuadPatternPosition</code>
 * @param set a LinkedHashset of Strings
 */
public static void printSetString(LinkedHashSet<String> set){
    if(set == null){
        System.out.println("Wrong parameter in function printLinkedHashSet");
        return;
    }


    Iterator itr = set.iterator();
    String value;

    while(itr.hasNext()){
        value =  (String)itr.next();
        System.out.println(value);
    }

}

/**
 * Prints the contents of a set that consists of </code>QuadPatternPosition</code>
 * @param set a LinkedHashset of Integers
 */
public static void printSetInteger(Set<Integer> set){
    if(set == null){
        System.out.println("Wrong parameter in function printSetInteger");
        return;
    }


    Iterator itr = set.iterator();
    Integer value;

    while(itr.hasNext()){
        value =  (Integer)itr.next();
        System.out.println(value);
    }

}


/**
 * This function prints all the pairs (variable-QuadPatternPositions) that exists in the HashMap
 * varsInWhereClause
 * @param varsInWhereClause a Hashmap where the variables in Where clause are the keys and the QuadPatternPosition
 * that a specific variable appears are organized in an ArrayList
 */
public static void printComponents(HashMap<String,HashMap<Integer,LinkedHashSet<QuadPatternPosition>>> varsInWhereClause){
    System.out.println("");
    System.out.println(">>>>>> Variables Table <<<<<<");
    String varKey;
    Integer unionKey = null;
    HashMap<Integer,LinkedHashSet<QuadPatternPosition>> values;
    LinkedHashSet<QuadPatternPosition> positions = null;

    for (Entry<String,HashMap<Integer,LinkedHashSet<QuadPatternPosition>>> e: varsInWhereClause.entrySet()) {
        varKey =  e.getKey();
       // values = new ArrayList<QuadPatternPosition>();
        values = e.getValue();
        for (Entry<Integer,LinkedHashSet<QuadPatternPosition>> en: values.entrySet()) {
             unionKey = en.getKey();
             positions = en.getValue();

        System.out.print("Variable "+varKey + " in graph pattern "+ unionKey +" has values {");
        printSetPos(positions);
        System.out.println("}");
        }
    }
    System.out.println(">>>>>>>>>>>>>><<<<<<<<<<<<<<<<");
    System.out.println("");
}

/**
 * This function prints all the pairs (variable-QuadPatternPositions) that exists in the HashMap
 * varsInWhereClause
 * @param evaluationResults  a Hashmap where the union and join identifier of a quad pattern is the key 
 */
public static void printEvaluationResultsTable(HashMap<Integer,LinkedHashSet<Integer>> evaluationResults){
    System.out.println("");
    System.out.println(">>>>>> Evaluation Table <<<<<<");
    Integer key;
    LinkedHashSet<Integer> values;

    for (Entry<Integer,LinkedHashSet<Integer>> e: evaluationResults.entrySet()) {
        key =  e.getKey();
       // values = new ArrayList<QuadPatternPosition>();
        values = e.getValue();
        

        System.out.println("KEy "+key + "has values: ");
        printSetInteger(values);
        
    }
    System.out.println(">>>>>>>>>>>>>><<<<<<<<<<<<<<<<");
    System.out.println("");
}

public static void printQuadPattern(QuadPattern qp){
    if(qp == null){
        System.out.println("Quadpattern parameter is null in printQuadPattern function!");
        return;
    }
    
    System.out.println("s: "+qp.getSubject()+" p: "+qp.getPredicate()+" o: "+qp.getObject()+" g: "+qp.getGraph());
}

/**
 * This function transforms an int position to a String position, i.e., 0->s, 1->p, 2->o
 * @param position an int between 0...2
 * @return "s" if position is 0, "p" if position is 1 and "o" if position is 2
 */
public static String IntToStringPos(int position){
    String pos = null;
    if (position>3 || position <0){
        System.out.println("No such position exists!Please check you have inserted a proper number (0-2)!");
        return null;
    }
    switch(position){
        case 0:
            pos = "s";
            break;
        case 1:
            pos = "p";
            break;
        case 2:
            pos = "o";
            break;
        default: break;
    }
    return pos;
}

/**
 * This function transforms an int position to a String position, i.e., 0->subject, 1->predicate, 2->object
 * @param position an int between 0...2
 * @return "subject" if position is 0, "predicate" if position is 1 and "object" if position is 2
 */
public static String IntToFullStringPos(int position){
    String pos = null;
    if (position>3 || position <0){
        System.out.println("No such position exists!Please check you have inserted a proper number (0-2)!");
        return null;
    }
    switch(position){
        case 0:
            pos = "subject";
            break;
        case 1:
            pos = "predicate";
            break;
        case 2:
            pos = "object";
            break;
        default: break;
    }
    return pos;
}



/**
 *Checks if a LinkedHashSet contains a </code>QuadPatternID</code> 
 * @param qp_ID
 * @param set
 * @return true if exists, false otherwise
 */
public static boolean checkLHSetForDublicates(QuadPatternID qp_ID, LinkedHashSet<QuadPatternID> set){
   Iterator itr = set.iterator();
   int union=qp_ID.getUnionNo();
   int join = qp_ID.getJoinNo();
   QuadPatternID id;
   while(itr.hasNext()){
       id = (QuadPatternID) itr.next();
       if(union == id.getUnionNo() && join == id.getJoinNo()){
           return true;
       }
   }

   return false;
}

/**
 *Checks if a LinkedHashSet contains a </code>QuadPatternPosition</code> with the same
 * </code>QuadPatternID</code> 
 * @param qp_ID
 * @param set
 * @return true if exists, false otherwise
 */
public static boolean checkLHSetForDublicates(QuadPatternPosition pos, LinkedHashSet<QuadPatternPosition> set){
   Iterator itr = set.iterator();
   int union = pos.getQuadPatternID().getUnionNo();
   int join = pos.getQuadPatternID().getJoinNo();
   QuadPatternPosition qp_pos;
   while(itr.hasNext()){
       qp_pos = (QuadPatternPosition) itr.next();
       if(union == qp_pos.getQuadPatternID().getUnionNo() && join == qp_pos.getQuadPatternID().getJoinNo()){
           return true;
       }
   }

   return false;
}

/**
 *Checks if an ArrayList contains a </code>QuadPatternPosition</code>
 * @param qp_ID
 * @param array
 * @return true if exists, false otherwise
 */
public static boolean checkArrayListForDublicates(QuadPatternPosition pos, ArrayList<QuadPatternPosition> array){
   Iterator itr = array.iterator();
   int union=pos.getQuadPatternID().getUnionNo();
   int join = pos.getQuadPatternID().getJoinNo();
   QuadPatternPosition id;
   while(itr.hasNext()){
       id = (QuadPatternPosition) itr.next();
       if(union == id.getQuadPatternID().getUnionNo() && join == id.getQuadPatternID().getJoinNo()){
           return true;
       }
   }

   return false;
}




/**
 * This function transforms a QuadPatternPosition Object to the same value represented as a String
 * @param varsub element to be tranformed
 * @return the String representation of a QuadPatternPosition
 */
public static String QuadPatternPositionToString(QuadPatternPosition varsub){
    return varsub.getQuadPatternID().getUnionNo()+"_"+varsub.getQuadPatternID().getJoinNo()+"."+varsub.getPosition();  
}


/**
 * This function transforms a String to the same value represented as a </code>QuadPatternPosition</code>
 * @param qp_pos a String of the form i^j.pos
 * @return a </code>QuadPatternPosition> with union i, join j and position pos
 */
public static QuadPatternPosition StringToQuadPattPos(String qp_pos){
    if(qp_pos == null){
        System.out.println("Parameter can not be null!");
        return null;
    }
    QuadPatternPosition pos = new QuadPatternPosition();
    QuadPatternID id = new QuadPatternID();
    id.setUnionNo(Integer.parseInt(qp_pos.substring(0, 1)));//union
    id.setJoinNo(Integer.parseInt(qp_pos.substring(2, 3))); //join
    pos.setPosition(qp_pos.substring(4));
    pos.setQuadpatternID(id);

    return pos;
}

/**
 * This function finds the QuadPattern that is assigned to QuadPatternID qp^i_j in WhereClause arraylist that is sorted in ascending order.
 * The function uses the binary search algorithm twice; once for the union subscript and once for the join subscript.
 * @param i the union subscript of QuadPatternID
 * @param j the join subscript of QuadPatternID
 * @return the quad pattern as a String
 */
/*
public static QuadPattern getQuadPattern(QuadPatternID id, HashMap<Integer,QuadPattern> whereClause){
    //String quadPattern;
    QuadPattern qp;
    int index;

    index = Collections.binarySearch(whereClause,(Integer)myHashFunction(id.getUnionNo(), id.getJoinNo()));
   // System.out.println("index of parsedQP:" + index);
    qp = new QuadPattern(WhereClause.get(index).getQuadPattern().getSubject(),
            WhereClause.get(index).getQuadPattern().getPredicate(),
            WhereClause.get(index).getQuadPattern().getObject(),
            WhereClause.get(index).getQuadPattern().getGraph());
    //System.out.println("parsedQP: "+ parsedQP.getSubject() +", "+ parsedQP.getPredicate() +", "+ parsedQP.getObject() +", "+ parsedQP.getGraph());
    return qp;
 }*/ //Den katalavaino gt to ftiaksa auto


/**
 * This function constructs and executes an SQL query based on the components of a quad pattern
 * @param qp a quadpattern that belongs to the graph pattern of the WHERE clause
 * @return the new query in the form of "select quad_ID from quad_ID from DB.DBA.MYQUADS where ...."
 */
public static String translateQuadPatternToSQLQuery(QuadPattern qp){
    String newQuery;
    if(qp == null){
        System.out.println("Error while trying to translate an update!Please check for null parameters!");
        return null;
    }
    String s, p, o, g;
    s = qp.getSubject();
    p = qp.getPredicate();
    o = qp.getObject();
    g = qp.getGraph();
    newQuery = "select quad_ID from DB.DBA.MYQUADS where";
    if(s.startsWith("<") || s.startsWith("\"")){
         newQuery = newQuery+" subject = '"+ s+"' and";
    }
    if(p.startsWith("<") || p.startsWith("\"")){
         newQuery = newQuery + " predicate = '"+ p + "' and ";
    }
    if(o.startsWith("<")|| o.startsWith("\"")){
         newQuery = newQuery + " object = '"+ o + "' and";
    }
        newQuery = newQuery + " graph = '"+ g + "'";
   // System.out.println("End of translateQuadPatternToSQL function!");
    return newQuery;
}

/**
 * This function computes the union superscript (i) existing in a quad pattern qp^i_j
 * @param key the KEY of the used hash function
 * @param N the limit of two numbers
 * @return the union superscript i
 */
public static int getUnionScript(int key){
    int i;
    i = key%N;
    //System.out.println("i: "+i);
    return i;
}

/**
 * This function computes the join subscript (j) existing in a quad pattern qp^i_j
 * @param key the KEY of the used hash function
 * @param N the limit of two numbers
 * @return the join subscript j
 */
public static int getJoinScript(int key){
    int j;
    j = key/N;
    //System.out.println("j: "+j);
    return j;
}

/**
 * This function computes a hash key based on two integer numbers that represent the union and join number of
 * a QuadPatternID
 * @param i the union number. Always greater than zero.
 * @param j the join number. Always greater than zero.
 * @return a hash key
 */
public static int myHashFunction(int i, int j){
    int key;
    key = i + j * N;
    //System.out.println("Key: "+ key);
    return key;
}

/**
*Printing the values of a Map. 
* @param map a Map
*/
public static void printMyMap(Map map){
Iterator it = map.keySet().iterator();
        Integer key;
        String tmp[];
        while(it.hasNext()){
            key= (Integer)it.next();
            //System.out.println("Key: ("+getUnionScript(key)+" , "+getJoinScript(key)+")");
            tmp = (String[])map.get(key);
            System.out.println("Value: ["+ tmp[0]+ " , "+ tmp[1]+ " , "+ tmp[2]+ " , "+ tmp[3]+"]");
            System.out.println("");
        }
}

/**
 * Mallon einai idia me tin printMyMap
 * @param map
 * @return
 */
public static String printMyHash(HashMap map){
Iterator it = map.keySet().iterator();
        Integer key;
        String upd = "";
        int counter = 0;
        QuadPattern tmp = null;
        while(it.hasNext()){
            counter++;
            key= (Integer)it.next();
            tmp = (QuadPattern)map.get(key);
            upd = upd + "("+ tmp.getSubject()+ " , "+ tmp.getPredicate()+ " , "+ tmp.getObject()+ " , "+ tmp.getGraph()+")";
            if(counter<map.size()){
                upd = upd +" . ";
            }
            
        }
        return upd;
        
}


/**
 * This function converts a ResultSet to an Array of integers
 * @param result the ResultSet from the execution of an SQL query
 * @return an array of integers that represent the quad_IDs appear in the ResultSet
 * @throws SQLException
 */
public static int[] ResultSetToIntArray(ResultSet result) throws SQLException{

     int size = 0;
    // ResultSetMetaData metaData = result.getMetaData();
    // int columnNo = metaData.getColumnCount();

        ArrayList<Integer> quadsID = new ArrayList<Integer>();

    while(result.next()){
         size++;
         quadsID.add(result.getInt(1));
     }

     if(size == 0){
         System.out.println("The input result set is empty!Please check Virtuoso for possible quadruples that match the quad pattern! ");
     }
     //int[] quadIDs = new int[size];

     for (int i = 0; i < size; i++){
         System.out.println("quadsID "+ i+ ": "+quadsID.get(i));
     }

     int resultArray[] = new int [size];
     for (int i = 0; i < quadsID.size(); i++) {
        resultArray[i] = quadsID.get(i);

    }

    return resultArray;
}

/**
 * This function converts a ResultSet to an Array of integers
 * @param result the ResultSet from the execution of an SQL query
 * @return an array of integers that represent the quad_IDs appear in the ResultSet
 * @throws SQLException
 */
public static LinkedHashSet ResultToLinkedHashSet(ResultSet result) throws SQLException{

     if(result == null){
         System.out.println("Wrong parameter in function ResultToLinkedHashSet!");
         return null;
     }
    LinkedHashSet resultSet = new LinkedHashSet();
    while(result.next()){
         resultSet.add(result.getInt(1));
     }

     if(resultSet.isEmpty()){
         System.out.println("The input result set is empty!Please check Virtuoso for possible quadruples that match the quad pattern! ");
     }
    
     return resultSet;
}

public static String JoinSubToString(ArrayList<QuadPatternPosition> joinsubs){
    String joinsub = "{";
    for (int i = 0; i < joinsubs.size(); i++) {
        joinsub = joinsub+QuadPatternPositionToString(joinsubs.get(i))+",";
    }
    
    return joinsub.substring(0, joinsub.length()-1)+"}";
}

/**
 * This function converts a LinkedHashSet to an ArrayList
 * @param set a LinkedHashSet of undefined objects
 * @return an ArrayList of objects
 * 
 */
public static ArrayList SetToArrayList(Set set){
    if(set == null){
         System.out.println("Wrong parameter in function LinkedHashSetToArrayList!");
         return null;
     }

    ArrayList list = new ArrayList();
    
    if(set.isEmpty()){
         System.out.println("The input set is empty! ");
         return list;
     }
    else{
         list.addAll(set);
    }
     return list;
}

public static ArrayList findQPPositions(QuadPatternID qp_id, ArrayList<QuadPatternPosition> joinPositions){
    if(qp_id == null || joinPositions == null){
        System.out.println("Null parameter in function findQPPositions!");
        return null;
    }
    ArrayList<QuadPatternPosition> matchPositions = new ArrayList<QuadPatternPosition>();
    for (int i = 0; i < joinPositions.size(); i++) {
        if(qp_id.compareTo(joinPositions.get(i).getQuadPatternID())==0){
            matchPositions.add(new QuadPatternPosition(new QuadPatternID(qp_id.getUnionNo(), qp_id.getJoinNo()), joinPositions.get(i).getPosition()));
        }
    }
    return matchPositions;
}

/**
 * This function returns the value that appears in a specific position of a quad pattern
 * @param qp_ID the quad pattern
 * @param pos the position
 * @return the variable
 */
public static String getPositionValue(QuadPatternID qp_ID, String pos){
    if(qp_ID == null || pos == null){
        System.out.println("Null parameters in getVariable function!");
        return null;
    }
    int unionNo = qp_ID.getUnionNo();
    int joinNo = qp_ID.getJoinNo();
    ParsedQP parsed_qp = new ParsedQP(qp_ID, null);
   // QuadPattern qp = getParsedQP(parsed_qp);
    return null;

}

public static void addAll(ListIterator it, Set<QuadPatternPosition> set){
    if(it == null || set == null){
        return;
    }
    Iterator itr = set.iterator();
    QuadPatternPosition pos;
    while(itr.hasNext()){
        pos = (QuadPatternPosition)itr.next();
        //System.out.println("index1: "+it.nextIndex());
        it.add(new VisitedQuadPatternPos(new QuadPatternPosition(new QuadPatternID(pos.getQuadPatternID().getUnionNo(), pos.getQuadPatternID().getJoinNo()), pos.getPosition()), 1));
       // System.out.println("pos: "+pos.StringQuadPatternPosition());
       // System.out.println("index2: "+it.nextIndex());
       // System.out.println("indexpx1 "+it.previousIndex());
        it.previous();
     //   System.out.println("index3 "+it.nextIndex());
    }
    
}

public static void addAll(ArrayList list, Set<QuadPatternPosition> set){
    if(list == null || set == null){
        return;
    }
    Iterator itr = set.iterator();
    QuadPatternPosition pos;
    while(itr.hasNext()){
        pos = (QuadPatternPosition)itr.next();
      
        list.add(new VisitedQuadPatternPos(pos, 0));
       // System.out.println("pos: "+pos.StringQuadPatternPosition());
       // System.out.println("index2: "+it.nextIndex());
       // System.out.println("indexpx1 "+it.previousIndex());
       
     //   System.out.println("index3 "+it.nextIndex());
    }

}

/**
 * This function returns the element that appears in the position PosNo
 * of the LinkedHashSet set
 * @param PosNo take values from 0 to set.size()
 */
public static Object get(int PosNo,LinkedHashSet set){
    if(set == null){
        return null;
    }
    Iterator itr = set.iterator();
    Object obj = new Object();
    int i = 0;
    while(itr.hasNext() && i <=PosNo){
        if(i == PosNo){
        obj = (Object) itr.next();
        break;
        }
        i++;
    }
    return obj;
        
}
        
}
