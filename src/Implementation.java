/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package NewVersionProvenance;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Argyro Avgoustaki
 * @email argiro@ics.forth.gr
 */

public class ImplementationTest2 {

//This HashMap denotes the assigned quad patterns of Where clause
//private static HashMap<Integer,QuadPattern> WhereClause = new HashMap<Integer,QuadPattern>();
//The Insert clause
private static String[] InsertClause = new String[4]; 
//The hashmap contains as key all the variables assigned to every quad pattern position that the specific variable exists, e.g., ?a -> (1,qp^1_1), (2,qp^2_2) etc
private static HashMap<String,HashMap<Integer,LinkedHashSet<QuadPatternPosition>>> varsInWhereClause = new HashMap<String, HashMap<Integer,LinkedHashSet<QuadPatternPosition>>>();
//The Hashmap contains all the ResultSet from the evaluation of each quad pattern in the WHERE clause
private static HashMap<Integer,LinkedHashSet<Integer>> evaluationResults = new HashMap<Integer, LinkedHashSet<Integer>>();
private static LinkedHashMap<Integer,QuadPattern> whereClause = new LinkedHashMap<Integer, QuadPattern>();
//The number of graph patterns in the WHERE clause
private static int graphPatternNo = 0;
private static LinkedHashMap<Integer,QuadPatternPosition> RecVarSubs;
private static LinkedHashMap<Integer,ArrayList<ArrayList<QuadPatternPosition>>> RecJoinSubs;
//private static LinkedHashSet<Integer> RecQuadIDs; me kathe epifilaksi oti mporei na mi doulepsei sto join
private static ArrayList<Integer> RecQuadIDs;
private static LinkedHashSet<QuadPatternID> SubscriptPatterns;
private static ArrayList<String> PeGraphs;
private static String [] RecInsertClause = new String[4];
private static LinkedHashMap<Integer,QuadPattern> RecUpdate;
private static HashMap<Integer,ArrayList<ArrayList<QuadPatternPosition>>> joinSubs;
private static ArrayList<ArrayList<Integer>> JoinIDs;


/**
 * This function parsing a SPARQL INSERT update and create the individual components of the
 * quad pattern in the INSERT clause and the quad patterns in the WHERE clause.
 * Therefore, an array (InsertClause) and two hashmaps (varsInWhereClause,whereClause) are constructed.
 * The array represents the attributes of the quad pattern in the INSERT clause.
 * The first HashMap represents the variables that appear in the Where clause and more specifically the QuadpatternPosition
 * that they appear. The second Hashmap represents a corresponding quadpattern identifying by the integer
 * hashkey (composed of the union and join scripts)
 * @param update a SPARQL INSERT update
 */
public static void createComponents(String update) throws SQLException{
    String[] tmpQuadPattern = new String[4];
    QuadPattern qp;
    String query;
    int k=0, token = 0, unionScript = 1, joinScript = 1, hashkey;
    Matcher m = Pattern.compile("(<[^<>]*>|\"[^\"\"]*\"|UNION|\\?[^\\s|\\,])").matcher(update);
    
    while(m.find()){/*Parsing the Update*/
        //System.out.println("mfind: "+m.group(0));
        if(token<4){/*Find the components of INSERT clause*/
            InsertClause[token] = m.group(0);
        }
        else{/*Find the components of WHERE clause*/
            
            if(m.group(0).equals("UNION")){/*There is a UNION operator in the WHERE clause*/
            unionScript++;
            joinScript = 1;
            continue;
            }

            /*If the element is a variable then update the varsInWhereClause table*/
            if(m.group(0).startsWith("?")){
                /*The variable is not already in the hashmap.
                 A new interior Hashmap for this variable must be created.*/
                if(varsInWhereClause.get(m.group(0)) == null){ 
                     varsInWhereClause.put(m.group(0), new HashMap<Integer, LinkedHashSet<QuadPatternPosition>>());
                }
                /*The variable appears for first time in this graph pattern*/
                if(varsInWhereClause.get(m.group(0)).get(unionScript) == null){
                   varsInWhereClause.get(m.group(0)).put(unionScript, new LinkedHashSet<QuadPatternPosition>());
                }
               varsInWhereClause.get(m.group(0)).get(unionScript).add(new QuadPatternPosition(new QuadPatternID(unionScript, joinScript), Utilities.IntToStringPos(k)));
            }
            tmpQuadPattern[k] = m.group(0);
            
            if(k == 3){//New quad pattern
              qp = new QuadPattern(tmpQuadPattern[0], tmpQuadPattern[1], tmpQuadPattern[2], tmpQuadPattern[3]);
              hashkey = Utilities.myHashFunction(unionScript, joinScript);
              whereClause.put(hashkey, qp);
              Utilities.printQuadPattern(qp);
              query = Utilities.translateQuadPatternToSQLQuery(qp);
              LinkedHashSet Res = Utilities.ResultToLinkedHashSet(VirtuosoOperations.executeSQLQuery(query));
              evaluationResults.put(hashkey,Res);
                
              k = -1 ;
              joinScript++;
            }
            k++;
        }
      token++;
    }
    graphPatternNo = unionScript;
   
}

/**
 * This function creates the MatchingPattern set (MP). MP contains all the quad pattern positions (QuadPatternPosition)
 * that are related directly or indirectly with a given variable in a specific graph pattern
 * @param variable a variable of the quad pattern in the INSERT clause of a SPARQL INSERT update
 * @param graphpattern the number of the graph pattern (greater to 1)
 * @return an ArrayList of  </code>QuadPatternPosition</code>
 */
public static TreeSet createMatchingPatterns(int position,String variable, int graphpattern){
    QuadPatternPosition qp_pos,pos;
    QuadPattern qp;
    QuadPatternID joinOp2;
    int hashkey = 0, hashkey2=0, joinNo = 0;
    int MaxSize, i = 0;
    ArrayList<QuadPatternPosition> temporal_MP = new ArrayList<QuadPatternPosition>();
    TreeSet<QuadPatternID> MP = new TreeSet<QuadPatternID>();
    Iterator itr;
    ArrayList<QuadPatternPosition> joinsub1 = new ArrayList<QuadPatternPosition>();
    ArrayList<QuadPatternPosition> joinsub2 = new ArrayList<QuadPatternPosition>();
    ArrayList<QuadPatternID> joinOp1;
    
     
    if(varsInWhereClause.get(variable).get(graphpattern)!= null){
        
        //Direct reference->Copy
        temporal_MP.addAll(varsInWhereClause.get(variable).get(graphpattern));
       /* if(temporal_MP.size() == 1){
            MP.add(new QuadPatternID(temporal_MP.get(0).getQuadPatternID().getUnionNo(),temporal_MP.get(0).getQuadPatternID().getJoinNo()));
            return MP;
        }*/
       // MP.add(new QuadPatternID(temporal_MP.get(0).getQuadPatternID().getUnionNo(),temporal_MP.get(0).getQuadPatternID().getJoinNo()));
        //Indirect reference->Join
        MaxSize = temporal_MP.size();
        while(i <MaxSize){
            qp_pos = temporal_MP.get(i);
            hashkey = Utilities.myHashFunction(qp_pos.getQuadPatternID().getUnionNo(),qp_pos.getQuadPatternID().getJoinNo());
            qp = whereClause.get(hashkey);
            if(MP.contains(qp_pos.getQuadPatternID())){
                    i++; 
                 //   System.out.println("Element: "+ qp_pos.StringQuadPatternPosition());
                    continue;
            }else{
               // System.out.println("qp:"+qp.getSubject());
                MP.add(new QuadPatternID(qp_pos.getQuadPatternID().getUnionNo(),qp_pos.getQuadPatternID().getJoinNo()));
            }
            /*Find the position of the variable in order to add in MP the quad pattern positions
            that the variables in the other two positions appear*/
            if(qp_pos.getPosition().matches("s")){
                if(Utilities.isVariable(qp.getPositionValue(1)) ){
                    temporal_MP.addAll(varsInWhereClause.get(qp.getPositionValue(1)).get(graphpattern));
                    MaxSize = temporal_MP.size();
                //Utilities.addAll(it, varsInWhereClause.get(qp.getPositionValue(1)).get(graphpattern));
                }
                if(Utilities.isVariable(qp.getPositionValue(2)) ){
                    temporal_MP.addAll(varsInWhereClause.get(qp.getPositionValue(2)).get(graphpattern));
                    MaxSize = temporal_MP.size();
                    //Utilities.addAll(it,varsInWhereClause.get(qp.getPositionValue(2)).get(graphpattern));
                   // System.out.println("edo"+ temporal_MP.get(temporal_MP.size()-1).StringQuadPatternPosition());
                }
            }else if(qp_pos.getPosition().matches("p")){
                if(Utilities.isVariable(qp.getPositionValue(0))){
                //Utilities.addAll(it,varsInWhereClause.get(qp.getPositionValue(0)).get(graphpattern));
                temporal_MP.addAll(varsInWhereClause.get(qp.getPositionValue(0)).get(graphpattern));
                    MaxSize = temporal_MP.size();
                }
                if(Utilities.isVariable(qp.getPositionValue(2))){
                    temporal_MP.addAll(varsInWhereClause.get(qp.getPositionValue(2)).get(graphpattern));
                    MaxSize = temporal_MP.size();
                //Utilities.addAll(it,varsInWhereClause.get(qp.getPositionValue(2)).get(graphpattern));
                }
            }else if(qp_pos.getPosition().matches("o")){
                if(Utilities.isVariable(qp.getPositionValue(0))){
                temporal_MP.addAll(varsInWhereClause.get(qp.getPositionValue(0)).get(graphpattern));
                    MaxSize = temporal_MP.size();
                    //Utilities.addAll(it,varsInWhereClause.get(qp.getPositionValue(0)).get(graphpattern));
                }
                if(Utilities.isVariable(qp.getPositionValue(1))){
                    temporal_MP.addAll(varsInWhereClause.get(qp.getPositionValue(1)).get(graphpattern));
                    MaxSize = temporal_MP.size();
                    //Utilities.addAll(it,varsInWhereClause.get(qp.getPositionValue(1)).get(graphpattern));
                }
            }
            i++;
        }
            
    }
  //  System.out.println("MP: "+MP.size());
    if(MP.size() == 1){
        return MP;
    }
  //  System.out.println("vgika");
   /* joinPositions = new ArrayList<QuadPatternPosition>();
   
    for (int j = 0; j < temporal_MP.size(); j++) {
        //pos = temporal_MP.get(j);
       // MP.add(new QuadPatternID(pos.getQuadPatternID().getUnionNo(), pos.getQuadPatternID().getJoinNo()));
       if(!joinPositions.contains(temporal_MP.get(j))){
       joinPositions.add(temporal_MP.get(j));
       
       }
       
    } ///Auto douleuei
    for (int k = 0; k < joinPositions.size(); k++) {
            System.out.println("testMP: "+joinPositions.get(k).StringQuadPatternPosition());
    }*/
    
    itr = MP.iterator();
    joinSubs.put(position, new ArrayList<ArrayList<QuadPatternPosition>>());
    
    if(itr.hasNext()){
       joinOp2= (QuadPatternID)itr.next();
      //System.out.println("joinOp2: "+joinOp2.getUnionNo()+ " , "+joinOp2.getJoinNo());
       joinOp1 = new ArrayList<QuadPatternID>();
       joinOp1.add(new QuadPatternID(joinOp2.getUnionNo(), joinOp2.getJoinNo()));
    }
    else{
        System.out.println("Null join subscripts in createMatchingPatterns function!");
        return null;
    }
    while(itr.hasNext()){  
        joinOp2 = (QuadPatternID)itr.next();
       // System.out.println("joinOP22: "+joinOp2.getJoinNo());
        hashkey2 = Utilities.myHashFunction(joinOp2.getUnionNo(), joinOp2.getJoinNo());
        for (int j = 0; j < joinOp1.size(); j++){ 
            hashkey = Utilities.myHashFunction(joinOp1.get(j).getUnionNo(), joinOp1.get(j).getJoinNo());
            for (int k = 0; k < 3; k++) {
                if(Utilities.isVariable(whereClause.get(hashkey).getPositionValue(k))){
               if((whereClause.get(hashkey).getPositionValue(k)).compareTo(whereClause.get(hashkey2).getSubject())==0 ){
                if(!checkArrayListForVariable(whereClause.get(hashkey).getPositionValue(k), joinsub1)){
                   joinsub1.add(new QuadPatternPosition(new QuadPatternID(joinOp1.get(j).getUnionNo(), joinOp1.get(j).getJoinNo()), Utilities.IntToStringPos(k)));
                   joinsub2.add(new QuadPatternPosition(new QuadPatternID(joinOp2.getUnionNo(), joinOp2.getJoinNo()), "s"));
                }
                   
                 //  System.out.println("mpika1");
                  // System.out.println("k: "+k+" "+whereClause.get(hashkey).getPositionValue(k)+ " , "+whereClause.get(hashkey2).getSubject() );
                  // System.out.println("joinsub1: "+joinsub1.get(joinsub1.size()-1).StringQuadPatternPosition());
                  // System.out.println("joinsub2: "+joinsub2.get(joinsub2.size()-1).StringQuadPatternPosition());
               }
            if((whereClause.get(hashkey).getPositionValue(k)).compareTo(whereClause.get(hashkey2).getPredicate())==0){
                if(!checkArrayListForVariable(whereClause.get(hashkey).getPositionValue(k), joinsub1)){
                joinsub1.add(new QuadPatternPosition(new QuadPatternID(joinOp1.get(j).getUnionNo(), joinOp1.get(j).getJoinNo()), Utilities.IntToStringPos(k)));
                joinsub2.add(new QuadPatternPosition(new QuadPatternID(joinOp2.getUnionNo(), joinOp2.getJoinNo()), "p"));
                }
                
               // System.out.println("mpika2");
               // System.out.println("k: "+k+" "+whereClause.get(hashkey).getPositionValue(k)+ " , "+whereClause.get(hashkey2).getPredicate());
               // System.out.println("joinsub1: "+joinsub1.get(joinsub1.size()-1).StringQuadPatternPosition());
                //System.out.println("joinsub2: "+joinsub2.get(joinsub2.size()-1).StringQuadPatternPosition());
            }
            if((whereClause.get(hashkey).getPositionValue(k)).compareTo(whereClause.get(hashkey2).getObject())==0){
                if(!checkArrayListForVariable(whereClause.get(hashkey).getPositionValue(k), joinsub1)){
                joinsub1.add(new QuadPatternPosition(new QuadPatternID(joinOp1.get(j).getUnionNo(), joinOp1.get(j).getJoinNo()), Utilities.IntToStringPos(k)));
                joinsub2.add(new QuadPatternPosition(new QuadPatternID(joinOp2.getUnionNo(), joinOp2.getJoinNo()), "o"));
                }
                
                //System.out.println("mpika3");
                //System.out.println("k: "+k+" "+whereClause.get(hashkey).getPositionValue(k)+ " , "+whereClause.get(hashkey2).getObject() );
                //System.out.println("joinsub1: "+joinsub1.get(joinsub1.size()-1).StringQuadPatternPosition());
                //System.out.println("joinsub2: "+joinsub2.get(joinsub2.size()-1).StringQuadPatternPosition());
            } 
            }       
            }
        }
        joinSubs.get(position).add(new ArrayList<QuadPatternPosition>());
        joinSubs.get(position).get(joinNo).addAll(joinsub1);
        joinSubs.get(position).add(new ArrayList<QuadPatternPosition>());
        joinSubs.get(position).get(joinNo+1).addAll(joinsub2);
        joinNo = joinNo +2;
        joinOp1.add(new QuadPatternID(joinOp2.getUnionNo(), joinOp2.getJoinNo()));
        joinsub1 = new ArrayList<QuadPatternPosition>();
        joinsub2 = new ArrayList<QuadPatternPosition>();
    }
    
   /* for (int j = 0; j < joinSubs.get(position).size(); j++) {
        for (int k = 0; k < joinSubs.get(position).get(j).size(); k++) {
            System.out.println("joinsub: "+j+" "+joinSubs.get(position).get(j).get(k).StringQuadPatternPosition());
        }
        
    }*/
       
    
    
    System.out.println("End of createMatchingPatterns function!");
       
    return MP;
}

/**
 *Checks if an ArrayList contains a specific variable in any of its </code>QuadPatternPosition</code>
 * @param variable the value of a variable
 * @param array an Arraylist of QuadPatternPositions
 * @return true if exists, false otherwise
 */
public static boolean checkArrayListForVariable(String variable, ArrayList<QuadPatternPosition> array){
   Iterator itr = array.iterator();
   int hashkey; 
   QuadPatternPosition id;
   while(itr.hasNext()){
      id = (QuadPatternPosition) itr.next();
      hashkey = Utilities.myHashFunction(id.getQuadPatternID().getUnionNo(), id.getQuadPatternID().getJoinNo());
      if(whereClause.get(hashkey).getPositionValue(id.getIntPosition()).compareTo(variable)==0){
         return true;
      }      
   }

   return false;
}


/**
 * This function finds the varsub of a position, i.e. the first occurence of the variable in the specified
 * graph pattern of the WHERE clause
 * @param variable a variable containing in a position of the INSERT clause
 * @param graphPattern the number of graph pattern what we are looking for the input variable
 * @return the first QuadPatternPosition in the graphPattern of WHERE clause that contains the input variable
 */
public static QuadPatternPosition getVarSubscript(String variable, int graphPattern){
    
    if(variable == null || graphPattern<1){
        System.out.println("The input variable is null!The varsub can not be computed!");
        return null;
    }
    
    if(!(variable.startsWith("?"))){
        System.out.println("The input parameter is not a variable!");
        return null;
    }
    //System.out.println("variable: "+variable);
   QuadPatternPosition varsub = null;
   Iterator itr = varsInWhereClause.get(variable).get(graphPattern).iterator();
   
    if(itr.hasNext()){
    varsub = (QuadPatternPosition)itr.next();
    }
    else{
        System.out.println("No var subscript can be created!");
        return null;
    }
      
  return varsub;
}


/**
 * This function returns a LinkedHashSet that contains all values of a specified position
 * @param quadIDs
 * @param pos
 * @return
 * @throws SQLException
 */
public static ArrayList findValueAttr(LinkedHashSet quadIDs, int pos) throws SQLException{
    if(quadIDs == null){
        System.out.println("QuadIDs parameter can not be null!");
        return null;
    }
    String position = null;
    ResultSet valueAttr;
    ArrayList<String> quadValue = new ArrayList<String>();
    switch(pos){
        case 0:
            position = "subject";
            break;
        case 1:
            position = "predicate";
            break;
        case 2:
            position = "object";
            break;
        default:
            break;
    }
    Iterator itr = quadIDs.iterator();
    while(itr.hasNext()){
       valueAttr = VirtuosoOperations.executeSQLQuery("select "+position+" from DB.DBA.MYQUADS where quad_ID = "+(Integer)itr.next());
       valueAttr.next();
       quadValue.add(valueAttr.getString(1));
    }

    return quadValue;
}

public static ArrayList findValueAttrJoin(LinkedHashMap<Integer,QuadPattern> values,int pos){
    QuadPattern qp = null;
   // System.out.println("mpika edo");
    ArrayList<String> quadValues = new ArrayList<String>();
    for (Integer key: values.keySet()){
        //System.out.println("edo?");
             qp = values.get(key);
             quadValues.add(qp.getPositionValue(pos));
    }
    return quadValues;
}


/**
 * Computes the cartesian product using the elements of three sets from the same quadpattern. The values of each set
 * are unique.
 * @param Values
 * @return
 */
public static LinkedHashMap cartesianProductOneQp(ArrayList<QuadPatternPosition> varsubscript,HashMap<Integer,ArrayList<String>> Values,HashMap<Integer,ArrayList<ArrayList<Integer>>> provValues){
    LinkedHashMap<Integer,VirtuosoEntry> composedQuads = new LinkedHashMap<Integer,VirtuosoEntry>();
    int noOfQuads=0;
    int i =0 ,j = 0, k = 0;
    int l = 0, m =0 ,n = 0;
    String s,p,o, prov_s, prov_p,prov_o;
     // System.out.println("varsubscript cart:"+varsubscript.get(0).StringQuadPatternPosition());
       while(i<Values.get(0).size()){
           s = Values.get(0).get(i);
           prov_s = provValues.get(0).get(l).get(0).toString();
           if(prov_s.compareTo("0") == 0 || prov_s.compareTo("Unknown") == 0){
                    prov_s = "Unknown";
           }  else{
               prov_s = Utilities.QuadPatternPositionToString(varsubscript.get(0))+"("+prov_s+")";
           }   
         //  System.out.println("prov_s: "+prov_s);
           composedQuads.put(noOfQuads, new VirtuosoEntry(new QuadPattern(s, null, null, null), new Provenance(prov_s, null, null))); 
           noOfQuads++;
           i++;
           l++;
       }
           
           noOfQuads = 0;
           while(j<Values.get(1).size()){
               p = Values.get(1).get(j);
               prov_p = provValues.get(1).get(m).get(0).toString();
               if(prov_p.compareTo("0") == 0 || prov_p.compareTo("Unknown") == 0){
                    prov_p = "Unknown"; 
                }else{
                   prov_p = Utilities.QuadPatternPositionToString(varsubscript.get(1))+"("+prov_p+")";
               }
               //System.out.println("prov_p: "+prov_p);
               composedQuads.get(noOfQuads).getQuadPattern().setPredicate(p);
               composedQuads.get(noOfQuads).getProv().setProvPosition(prov_p, 1);
               noOfQuads++;
               j++;
               m++;
           }
            
            noOfQuads = 0;
            while(k<Values.get(2).size()){
                o = Values.get(2).get(k);
                prov_o = provValues.get(2).get(n).get(0).toString();
                if(prov_o.compareTo("0") == 0 || prov_o.compareTo("Unknown") == 0){
                    prov_o = "Unknown";
                }else{
                    prov_o = Utilities.QuadPatternPositionToString(varsubscript.get(2))+"("+prov_o+")";
                }
               // System.out.println("prov_o:"+prov_o);
                composedQuads.get(noOfQuads).getQuadPattern().setObject(o);
                composedQuads.get(noOfQuads).getProv().setProvPosition(prov_o, 2);
                noOfQuads++;
                k++;
                n++;                
            }
           
    return composedQuads;
}

/**
 * Computes the cartesian product using the elements of three sets evaluated from two quad patterns. The values of each set
 * are unique.
 * @param Values
 * @return
 */
public static LinkedHashMap cartesianProductOneQpOnlyValues(ArrayList<QuadPatternPosition> varsubscript,HashMap<Integer,ArrayList<String>> Values){
    LinkedHashMap<Integer,QuadPattern> composedQuads = new LinkedHashMap<Integer,QuadPattern>();
    int noOfQuads=0;
    int i =0 ,j = 0, k = 0;
    String s,p,o;
       while(i<Values.get(0).size()){
           s = Values.get(0).get(i);
           composedQuads.put(noOfQuads,new QuadPattern(s, null, null, null)); 
           noOfQuads++;
           i++;
       }
           
           noOfQuads = 0;
           while(j<Values.get(1).size()){
               p = Values.get(1).get(j);
               composedQuads.get(noOfQuads).setPredicate(p);
               noOfQuads++;
               j++;
               
           }
            
            noOfQuads = 0;
            while(k<Values.get(2).size()){
                o = Values.get(2).get(k);
                composedQuads.get(noOfQuads).setObject(o);
                noOfQuads++;
                k++;
                                
            }
           
    return composedQuads;
}



/**
 * Computes the cartesian product using the elements of three sets. The values of each set
 * are unique.
 * @param Values
 * @param singleQp 0:if s is evaluated in different qp from p and o (p and o are evaluated through the same
 * qp), 1: if p is evaluated in different qp and 2: if o is evaluated in different qp
 * @return
 */
public static LinkedHashMap cartesianProductTwoQp(ArrayList<QuadPatternPosition> varsubscript,HashMap<Integer,ArrayList<String>> Values, HashMap<Integer,ArrayList<ArrayList<Integer>>> ProvValues,int singleQp){
    LinkedHashMap<Integer,VirtuosoEntry> composedQuads = new LinkedHashMap<Integer,VirtuosoEntry>();
    int noOfQuads=0;
    int i =0 ,j = 0, k = 0;
    int l = 0, m =0 ,n = 0;
    String s,p,o, prov_s,prov_p,prov_o;
    
       switch(singleQp){
           case 0:
               while(j<Values.get(1).size() && k<Values.get(2).size()){
                o = Values.get(2).get(k);
                p = Values.get(1).get(j);
                prov_o = ProvValues.get(2).get(n).get(0).toString();
                prov_p = ProvValues.get(1).get(m).get(0).toString();
                while(i<Values.get(0).size()){
                s = Values.get(0).get(i);
                prov_s = ProvValues.get(0).get(l).get(0).toString();
                if(prov_s.compareTo("0") == 0 || prov_s.compareTo("Unknown") == 0){
                    prov_s = "Unknown";
                }else{
                    prov_s = Utilities.QuadPatternPositionToString(varsubscript.get(0))+"("+prov_s+")";
                }
                if(prov_p.compareTo("0") == 0 || prov_p.compareTo("Unknown") == 0){
                    prov_p = "Unknown";
                }else{
                    prov_p = Utilities.QuadPatternPositionToString(varsubscript.get(1))+"("+prov_p+")";
                }
                if(prov_o.compareTo("0") == 0 || prov_o.compareTo("Unknown") == 0){
                    prov_o = "Unknown";
                }else{
                    prov_o = Utilities.QuadPatternPositionToString(varsubscript.get(2))+"("+prov_o+")";
                }
                composedQuads.put(noOfQuads, new VirtuosoEntry(new QuadPattern(s, p, o, null), new Provenance(prov_s, prov_p, prov_o)));
                noOfQuads++;
                i++;
                l++;
                }  
                i=0;
                l=0;
                k++;
                j++;
                n++;
                m++;
              }
               break;
           case 1:
              while(i<Values.get(0).size() && k<Values.get(2).size()){
                  //System.out.println("mpika edo ki edo");
                s = Values.get(0).get(i);
                  System.out.println("s: "+s);
                o = Values.get(2).get(k);
                  System.out.println("o: "+o);
                prov_s = ProvValues.get(0).get(l).get(0).toString();
                prov_o = ProvValues.get(2).get(n).get(0).toString();
                while(j<Values.get(1).size()){
                p = Values.get(1).get(j);
                prov_p = ProvValues.get(1).get(m).get(0).toString();
                if(prov_s.compareTo("0") == 0 || prov_s.compareTo("Unknown") == 0){
                    prov_s = "Unknown";
                }else{
                    prov_s = Utilities.QuadPatternPositionToString(varsubscript.get(0))+"("+prov_s+")";
                }
                if(prov_p.compareTo("0") == 0 || prov_p.compareTo("Unknown") == 0){
                    prov_p = "Unknown";
                }else{
                    prov_p = Utilities.QuadPatternPositionToString(varsubscript.get(1))+"("+prov_p+")";
                }
                if(prov_o.compareTo("0") == 0 || prov_o.compareTo("Unknown") == 0){
                    prov_o = "Unknown";
                }else{
                    prov_o = Utilities.QuadPatternPositionToString(varsubscript.get(2))+"("+prov_o+")";
                }
                composedQuads.put(noOfQuads, new VirtuosoEntry(new QuadPattern(s, p, o, null), new Provenance(prov_s, prov_p, prov_o)));
                  //  System.out.println("composed: "+ composedQuads.get(noOfQuads).getQuadPattern().getSubject()+" "+composedQuads.get(noOfQuads).getQuadPattern().getObject());
                noOfQuads++;
                j++;
                m++;
                }  
                j=0;
                m=0;
                 // System.out.println("vgika");
                  i++;
                  k++;
                  l++;
                  n++;
              }
               break;
           case 2:
               while(i<Values.get(0).size() && j<Values.get(1).size()){
                s = Values.get(0).get(i);
                p = Values.get(1).get(j);
                prov_s = ProvValues.get(0).get(l).get(0).toString();
                prov_p = ProvValues.get(1).get(m).get(0).toString();
                while(k<Values.get(2).size()){
                o = Values.get(2).get(k);
                prov_o = ProvValues.get(2).get(n).toString();
                if(prov_s.compareTo("0") == 0 || prov_s.compareTo("Unknown") == 0){
                    prov_s = "Unknown";
                }else{
                    prov_s = Utilities.QuadPatternPositionToString(varsubscript.get(0))+"("+prov_s+")";
                }
                if(prov_p.compareTo("0") == 0 || prov_s.compareTo("Unknown") == 0){
                    prov_p = "Unknown";
                }else{
                    prov_p = Utilities.QuadPatternPositionToString(varsubscript.get(1))+"("+prov_p+")";
                }
                if(prov_o.compareTo("0") == 0 || prov_s.compareTo("Unknown") == 0){
                    prov_o = "Unknown";
                }else{
                    prov_o = Utilities.QuadPatternPositionToString(varsubscript.get(2))+"("+prov_o+")";
                }
                composedQuads.put(noOfQuads, new VirtuosoEntry(new QuadPattern(s, p, o, null), new Provenance(prov_s, prov_p, prov_o)));
                noOfQuads++;
                k++;
                n++;
                }
                k=0;
                n=0;
                i++;
                j++;
                m++;
                l++;
              }
               break;
           default:
               break;
      
       }
   
    return composedQuads;
}

/**
 * Computes the cartesian product using the elements of three sets. The values of each set
 * are unique.
 * @param Values
 * @param singleQp 0:if s is evaluated in different qp from p and o (p and o are evaluated through the same
 * qp), 1: if p is evaluated in different qp and 2: if o is evaluated in different qp
 * @return
 */
public static LinkedHashMap cartesianProductTwoQpOnlyValues(ArrayList<QuadPatternPosition> varsubscript,HashMap<Integer,ArrayList<String>> Values,int singleQp){
    LinkedHashMap<Integer,QuadPattern> composedQuads = new LinkedHashMap<Integer,QuadPattern>();
    int noOfQuads=0;
    int i =0 ,j = 0, k = 0;
    String s,p,o;
    
       switch(singleQp){
           case 0:
               while(j<Values.get(1).size() && k<Values.get(2).size()){
                o = Values.get(2).get(k);
                p = Values.get(1).get(j);
                while(i<Values.get(0).size()){
                s = Values.get(0).get(i);
                composedQuads.put(noOfQuads, new QuadPattern(s, p, o, null));
                noOfQuads++;
                i++;
                }  
                i=0;
                k++;
                j++;
              }
               break;
           case 1:
              while(i<Values.get(0).size() && k<Values.get(2).size()){
                s = Values.get(0).get(i);
                o = Values.get(2).get(k);
                while(j<Values.get(1).size()){
                p = Values.get(1).get(j);
                composedQuads.put(noOfQuads, new QuadPattern(s, p, o, null));
                noOfQuads++;
                j++;
               
                }  
                j=0;
               
                  i++;
                  k++;
                  
                  
              }
               break;
           case 2:
               while(i<Values.get(0).size() && j<Values.get(1).size()){
                s = Values.get(0).get(i);
                p = Values.get(1).get(j);
                while(k<Values.get(2).size()){
                o = Values.get(2).get(k);
                composedQuads.put(noOfQuads, new QuadPattern(s, p, o, null));
                noOfQuads++;
                k++;
                
                }
                k=0;
                
                i++;
                j++;
              }
               break;
           default:
               break;
      
       }
   
    return composedQuads;
}

/**
 * Computes the cartesian product using the elements of three sets. The values of each set
 * are unique.
 * @param Values
 * @return
 */
public static LinkedHashMap cartesianProduct(ArrayList<QuadPatternPosition> varsubscript,HashMap<Integer,ArrayList<String>> Values,HashMap<Integer,ArrayList<ArrayList<Integer>>> ProvValues){
    LinkedHashMap<Integer,VirtuosoEntry> composedQuads = new LinkedHashMap<Integer,VirtuosoEntry>();
    int i =0 ,j = 0, k = 0;
    int l = 0, m =0 ,n = 0;
    int noOfQuads = 0, q = 1,h=0;
    String s,p,o, prov_s= null,prov_p=null,prov_o=null;
       
       while(i<Values.get(0).size()){
           s = Values.get(0).get(i);
           prov_s = ProvValues.get(0).get(l).get(0).toString();
           if(ProvValues.get(0).get(l).size()>1){
               while(q <ProvValues.get(0).get(l).size()) {
                   prov_s = prov_s +Utilities.JoinSubToString(joinSubs.get(0).get(h))+Utilities.JoinSubToString(joinSubs.get(0).get(h+1))+ProvValues.get(0).get(l).get(q).toString();
                   h = h+2;
                   q++;
               }
           }
          // System.out.println("prov_s: "+prov_s);
           while(j<Values.get(1).size()){
               p = Values.get(1).get(j);
            //   System.out.println("p:"+p);
               
               prov_p = ProvValues.get(1).get(m).get(0).toString();
               if(ProvValues.get(1).get(m).size()>1){
                   q = 1;
                   h=0;
               while(q <ProvValues.get(1).get(m).size()) {
                   prov_p = prov_p +Utilities.JoinSubToString(joinSubs.get(1).get(h))+Utilities.JoinSubToString(joinSubs.get(1).get(h+1))+ProvValues.get(1).get(m).get(q).toString();
                   h = h+2;
                   q++;
                }
                }
               while(k<Values.get(2).size()){
                   o = Values.get(2).get(k);
                   prov_o = null;
                   prov_o = ProvValues.get(2).get(n).get(0).toString();
                   if(ProvValues.get(2).get(n).size()>1){
                   q = 1;
                   h=0;
               while(q <ProvValues.get(2).get(n).size()) {
                   prov_o = prov_o +Utilities.JoinSubToString(joinSubs.get(2).get(h))+Utilities.JoinSubToString(joinSubs.get(2).get(h+1))+ProvValues.get(2).get(m).get(q).toString();
                   h = h+2;
                   q++;
               }
                }
              //     System.out.println("o: "+o);
               //    System.out.println("prov_o"+prov_o);
                 if(prov_s.compareTo("0") == 0 || prov_s.compareTo("Unknown") == 0){
                    prov_s = "Unknown";
                 }else if (!prov_s.startsWith(varsubscript.get(0).StringQuadPatternPosition()+"(")){
                    prov_s = Utilities.QuadPatternPositionToString(varsubscript.get(0))+"("+prov_s+")";
                 }  
                   System.out.println("prov_s: "+prov_s);
                if(prov_p.compareTo("0") == 0 || prov_p.compareTo("Unknown") == 0){
                    prov_p = "Unknown";
                }else if (!prov_p.startsWith(varsubscript.get(1).StringQuadPatternPosition()+"(")){
                    prov_p = Utilities.QuadPatternPositionToString(varsubscript.get(1))+"("+prov_p+")";
                }
                //   System.out.println("prov_p"+prov_p);
                if(prov_o.compareTo("0") == 0 || prov_o.compareTo("Unknown") == 0){
                    prov_o = "Unknown";
                }else{
                    prov_o = Utilities.QuadPatternPositionToString(varsubscript.get(2))+"("+prov_o+")";
                }
                 
                 composedQuads.put(noOfQuads,new VirtuosoEntry(new QuadPattern(s, p, o, null), new Provenance(prov_s, prov_p, prov_o)));
                 noOfQuads++;
                 k++;
                 n++;
               }
               k=0;
               n=0;
               j++;
               m++;
           }
           j=0;
           m=0;
           i++;
           l++;
       }
    
    return composedQuads;

}

    /**
     * Computes the cartesian product using the elements of three sets.The values of each set
 are unique.
     * @param varsubscript
     * @param Values
     * @return
     */
public static LinkedHashMap cartesianProductOnlyValues(ArrayList<QuadPatternPosition> varsubscript,HashMap<Integer,ArrayList<String>> Values){
    LinkedHashMap<Integer,QuadPattern> composedQuads = new LinkedHashMap<Integer,QuadPattern>();
    int i =0 ,j = 0, k = 0;
    int noOfQuads = 0;
    String s,p,o;
       
       while(i<Values.get(0).size()){
           s = Values.get(0).get(i);
           while(j<Values.get(1).size()){
               p = Values.get(1).get(j);
               while(k<Values.get(2).size()){
                 o = Values.get(2).get(k);               
                 composedQuads.put(noOfQuads,new QuadPattern(s, p, o, null));
                 noOfQuads++;
                 k++;
                 
               }
               k=0;
               
               j++;
               
           }
           j=0;
           i++;
       }
    
    return composedQuads;
}

/**
 * Finds the entries of DD.DBA.MYQUADS that meets the join requirements
 */
private static LinkedHashMap findValuesJoin(LinkedHashSet<QuadPatternID> spe_pos, ArrayList<ArrayList<QuadPatternPosition>> joinsubs_pos, QuadPatternPosition varsub) throws SQLException{
    if(spe_pos == null || joinsubs_pos == null || varsub == null){
        System.out.println("Null parameter in findValuesJoin function!");
        return null;
    }
    
    Iterator itr = spe_pos.iterator();
    Iterator itr2 = spe_pos.iterator();
    QuadPatternID qp_id;
    String whereClsValue;
    String query = "select * from ";
    while(itr.hasNext()){
        qp_id = (QuadPatternID)itr.next();
        query = query +"DB.DBA.MYQUADS t"+Integer.toString(qp_id.getJoinNo())+", ";
    }
    
    query = query.substring(0, query.length()-2);//to remove the last comma
    query = query + " where ";
    
    for (int j = 0; j < joinsubs_pos.size(); j=j+2) {
        for (int k = 0; k< joinsubs_pos.get(j).size(); k++) {
            query = query + "t"+Integer.toString(joinsubs_pos.get(j).get(k).getQuadPatternID().getJoinNo())+"."+
                    joinsubs_pos.get(j).get(k).getVirtuosoPosition()+" = "+"t"+Integer.toString(joinsubs_pos.get(j+1).get(k).getQuadPatternID().getJoinNo())+"."+
                    joinsubs_pos.get(j+1).get(k).getVirtuosoPosition()+" and ";
                    
                    
        }
       query = query+ "t"+Integer.toString(joinsubs_pos.get(j).get(0).getQuadPatternID().getJoinNo())+".graph = '"+
                    whereClause.get(Utilities.myHashFunction(joinsubs_pos.get(j).get(0).getQuadPatternID().getUnionNo(),
                            joinsubs_pos.get(j).get(0).getQuadPatternID().getJoinNo())).getGraph()+"' and t"+Integer.toString(joinsubs_pos.get(j+1).get(0).getQuadPatternID().getJoinNo())+".graph = '"+
                    whereClause.get(Utilities.myHashFunction(joinsubs_pos.get(j+1).get(0).getQuadPatternID().getUnionNo(),
                            joinsubs_pos.get(j+1).get(0).getQuadPatternID().getJoinNo())).getGraph()+"' and " ;
    
    }
 
    while(itr2.hasNext()){
        qp_id = (QuadPatternID)itr2.next();
        for (int i = 0; i < 3; i++) {
           whereClsValue = whereClause.get(Utilities.myHashFunction(qp_id.getUnionNo(), qp_id.getJoinNo())).getPositionValue(i);
           if(!whereClsValue.startsWith("?")){
               query = query +"t"+Integer.toString(qp_id.getJoinNo())+"."+Utilities.IntToFullStringPos(i)+" = '"+ whereClsValue+"' and ";
           }  
        }
       
    }

    
    LinkedHashMap<Integer,LinkedHashMap<Integer,QuadPattern>> results = new LinkedHashMap<Integer, LinkedHashMap<Integer,QuadPattern>>();
    query = query.substring(0, query.length()-4);//to remove the last 'and'
    System.out.println("Query: "+query);
    ResultSet result = VirtuosoOperations.executeSQLQuery(query);
    ResultSetMetaData rsmd = result.getMetaData();
    System.out.println("columnNo: "+rsmd.getColumnCount());
    ArrayList<QuadPatternID> speVal = Utilities.SetToArrayList(spe_pos);
    JoinIDs = new ArrayList<ArrayList<Integer>>();
    //System.out.println("speVal: "+speVal.size());
    int i =1, idsNo = 0 ;
    while(result.next()){
        System.out.println("gb ff b");
        System.out.println("JOINIDS: ");
        JoinIDs.add(new ArrayList<Integer>());
        
        System.out.println("ngnge");
        i =1;
        for (int j = 0; j <speVal.size(); j++) {
            
            if(results.get(speVal.get(j).getJoinNo())==null){
                results.put(speVal.get(j).getJoinNo(), new LinkedHashMap<Integer,QuadPattern>());
            }
            
            if(results.get(speVal.get(j).getJoinNo()).get(result.getInt(i))==null){
            results.get(speVal.get(j).getJoinNo()).put(result.getInt(i),new QuadPattern(result.getString(i+1), result.getString(i+2), result.getString(i+3), result.getString(i+4)));
                System.out.println("edo");
            JoinIDs.get(idsNo).add(result.getInt(i));
            }
            
            
            i = i +5;
        }
        
        idsNo++;
   }

   // System.out.println("vgika");
    return results;
}

/**
 * This function computes the result quadruples for a graph pattern and their corresponding provenance.
 * After that they insert them to Virtuoso
 * @param varsubscript: each position in the ArrayList describes the var subscript of the corresponding position
 * @param spe: each position in the big TreeSet describes the set of QuadPatternIDs that will be used for the evaluation of the specific position
 * @param joinSubs:
 */
public static LinkedHashMap insertToVirtuoso(int[] MatchingPatterns, ArrayList<QuadPatternPosition> varsubscript, HashMap<Integer,LinkedHashSet<QuadPatternID>> spe, HashMap<Integer,ArrayList<ArrayList<QuadPatternPosition>>> joinSubs){
   if(varsubscript == null || spe == null || joinSubs == null){
       System.out.println("Var subscript, spe or joinSubs parameter is null in function insertToVirtuoso!");
       return null;
   }
    //Stores the hashkey for each position in order to find the quad_ids from evaluationResults table
    int[] QuadsIDs = new int[3];
    LinkedHashMap<Integer,VirtuosoEntry> Entries = new LinkedHashMap<Integer,VirtuosoEntry>();
    LinkedHashMap<Integer,LinkedHashMap<Integer,QuadPattern>> JoinPosVal = new LinkedHashMap<Integer,LinkedHashMap<Integer,QuadPattern>>();
    int hashkey = 0;
    QuadPatternID qpid;
    HashMap<Integer,ArrayList<String>> TripleValues = new HashMap<Integer,ArrayList<String>>();
    HashMap<Integer,ArrayList<ArrayList<Integer>>> ProvValues = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();
    ArrayList<Integer> tmpProv;
    for (int i = 0; i < 3; i++) {//For each position
      // System.out.println("varsub:"+ varsubscript.get(i).StringQuadPatternPosition());
        if(varsubscript.get(i) != null){//we have variable in the position i
          //  System.out.println("varsub: "+varsubscript.get(i).StringQuadPatternPosition());
            if(MatchingPatterns[i] == 1){ //Copy case
                System.out.println("mpika");
                qpid = (QuadPatternID)Utilities.get(0, spe.get(i));
                hashkey = Utilities.myHashFunction(qpid.getUnionNo(),qpid.getJoinNo());  
               
                    try {
                        TripleValues.put(i,findValueAttr(evaluationResults.get(hashkey), varsubscript.get(i).getIntPosition()));
                        ProvValues.put(i,new ArrayList<ArrayList<Integer>>());
                        tmpProv = Utilities.SetToArrayList(evaluationResults.get(hashkey));
                        for(int j = 0; j < tmpProv.size(); j++) {
                            ProvValues.get(i).add(new ArrayList<Integer>());
                            ProvValues.get(i).get(j).add(tmpProv.get(j));
                        }
                        QuadsIDs [i] = hashkey;
                        
                        
                    } catch (SQLException ex) {
                        System.out.println("Exception " + ex.getMessage() +" in insertToVirtuoso function!");
                    }
            }
            else{//Join case
                System.out.println("join");
                    try {
                    JoinPosVal = findValuesJoin(spe.get(i), joinSubs.get(i),varsubscript.get(i));
                   QuadPattern qpf;
                    for (Integer key: JoinPosVal.keySet()){

            LinkedHashMap<Integer,QuadPattern> value = JoinPosVal.get(key);
             for (Integer key2: value.keySet()){
                qpf = value.get(key2);
                System.out.println(key + " " + key2+" , ");
            Utilities.printQuadPattern(qpf);
             }
                    }
            TripleValues.put(i,findValueAttrJoin(JoinPosVal.get(varsubscript.get(i).getQuadPatternID().getJoinNo()),varsubscript.get(i).getIntPosition()));
                 for (int j = 0; j < TripleValues.get(i).size(); j++) {
                     System.out.println("element: "+TripleValues.get(i).get(j));
                 }
                 ProvValues.put(i,new ArrayList<ArrayList<Integer>>());
                        for(int j = 0; j < JoinIDs.size(); j++) {
                            ProvValues.get(i).add(new ArrayList<Integer>());
                            for (int k = 0; k < JoinIDs.get(j).size(); k++) {
                                ProvValues.get(i).get(j).add(JoinIDs.get(j).get(k)); 
                            }
                           
                        }            
 
                    
                    } catch (SQLException ex) {
                        System.out.println("Exception "+ex.getMessage()+ "in function findValueAttrJoin!");
                    }
                    
           
                
                 }
            
        }else{ //we have constant in the position i
             TripleValues.put(i,new ArrayList<String>());
             TripleValues.get(i).add(InsertClause[i]);
             ProvValues.put(i,new ArrayList<ArrayList<Integer>>());
             ProvValues.get(i).add(new ArrayList<Integer>()); 
            ProvValues.get(i).get(0).add(0);//0 represents the 'Unknown'
                           
             QuadsIDs[i] = 0;
             //System.out.println("i: "+i +" "+InsertClause[i]+ " "+ProvValues.get(i));
            // Utilities.printSetString(TripleValues.get(i));
             
        }
   }
   /* for (int k = 0; k < TripleValues.size(); k++) {
        for (int i = 0; i < TripleValues.get(k).size(); i++) {
            System.out.println("TripleValue: "+"i:"+i+TripleValues.get(k).get(i));
        }
    }*/
    
    if(QuadsIDs[0] == QuadsIDs[1] && QuadsIDs[0] != QuadsIDs[2]){ // subject and predicate are evaluated on the same quad pattern
        if(QuadsIDs[0]==0){

            Entries = cartesianProduct(varsubscript, TripleValues, ProvValues);
        }else{
            Entries = cartesianProductTwoQp(varsubscript,TripleValues,ProvValues,2);
        }
    }
    else if (QuadsIDs[0] == QuadsIDs[2] && QuadsIDs[0] != QuadsIDs[1]){//subject and object are evaluated on the same quad pattern
        if(QuadsIDs[0]==0){
            Entries = cartesianProduct(varsubscript, TripleValues, ProvValues);
        }else{
            Entries = cartesianProductTwoQp(varsubscript,TripleValues,ProvValues,1);
        }
    }
    else if (QuadsIDs[1] == QuadsIDs[2] && QuadsIDs[0] != QuadsIDs[1]) {//predicate and object are evaluated on the same quad pattern
        if(QuadsIDs[1]==0){
            Entries = cartesianProduct(varsubscript, TripleValues, ProvValues);
        }else{
        Entries = cartesianProductTwoQp(varsubscript,TripleValues,ProvValues,0);
        }
    }
    else if (QuadsIDs[0] == QuadsIDs[1] && QuadsIDs[0] == QuadsIDs[2]){//where clause has one quad pattern
        if(QuadsIDs[0]==0){
            Entries = cartesianProduct(varsubscript, TripleValues, ProvValues);
        }else{
        Entries = cartesianProductOneQp(varsubscript,TripleValues,ProvValues);
        }
    }
    else{ //if(QuadsIDs[0] != QuadsIDs[1] && QuadsIDs[0] != QuadsIDs[2] && QuadsIDs[1] != QuadsIDs[2]){
        Entries = cartesianProduct(varsubscript,TripleValues,ProvValues);
        
  
    }
     
    /*for (int i = 0; i < Entries.size(); i++) {
        System.out.println("Entries size"+Entries.size());
        tmpQp = Entries.get(i).getQuadPattern();
        System.out.println("qp: "+tmpQp.getSubject()+" ,"+tmpQp.getPredicate()+" "+tmpQp.getObject());
        quad_ID = VirtuosoOperations.getQuadID(tmpQp.getSubject(),  tmpQp.getPredicate(), tmpQp.getObject(), InsertClause[3]);
        if (quad_ID == 0) {//This is the first time that the quadruple is inserted in Virtuoso
            VirtuosoOperations.executeSQLUpdate("insert into DB.DBA.MYQUADS (subject,predicate,object,graph) values ('"+ tmpQp.getSubject()+"','"+
             tmpQp.getPredicate()+"','"+tmpQp.getObject()+"','"+InsertClause[3]+"')");
         }
         else{
             cpe = VirtuosoOperations.getMaxCPENumber(quad_ID);
         }
      newID = VirtuosoOperations.getQuadID(Entries.get(i).getQuadPattern().getSubject(), Entries.get(i).getQuadPattern().getPredicate(), Entries.get(i).getQuadPattern().getObject(), InsertClause[3]);
       // System.out.println("prov: "+Entries.get(i).getProv().getProvPosition(0)+Entries.get(i).getProv().getProvPosition(1)+Entries.get(i).getProv().getProvPosition(2));
        VirtuosoOperations.executeSQLUpdate("insert into DB.DBA.MyProvenance (quad_ID, cpe, pe, prov_s, prov_p, prov_o) values ("+newID+","+(cpe+1)+","+(pe+1)+",'"+Entries.get(i).getProv().getProvPosition(0)+"','"+Entries.get(i).getProv().getProvPosition(1)+"','"+Entries.get(i).getProv().getProvPosition(2)+"')");
     
    }*/
   
   
    return Entries;
}


/**
 * This function computes the result quadruples for a graph pattern and their corresponding provenance.
 * After that they insert them to Virtuoso
 * @param varsubscript: each position in the ArrayList describes the var subscript of the corresponding position
 * @param spe: each position in the big TreeSet describes the set of QuadPatternIDs that will be used for the evaluation of the specific position
 * @param joinSubs:
 */
public static LinkedHashMap insertToVirtuosoWithoutProv(int[] MatchingPatterns, ArrayList<QuadPatternPosition> varsubscript, HashMap<Integer,LinkedHashSet<QuadPatternID>> spe, HashMap<Integer,ArrayList<ArrayList<QuadPatternPosition>>> joinSubs){
   if(varsubscript == null || spe == null || joinSubs == null){
       System.out.println("Var subscript, spe or joinSubs parameter is null in function insertToVirtuoso!");
       return null;
   }
   // System.out.println("mpika");
    //Stores the hashkey for each position in order to find the quad_ids from evaluationResults table
    int[] QuadsIDs = new int[3];
    LinkedHashMap<Integer,QuadPattern> Entries = new LinkedHashMap<Integer,QuadPattern>();
    int hashkey = 0;
    QuadPatternID qpid;
    HashMap<Integer,ArrayList<String>> TripleValues = new HashMap<Integer,ArrayList<String>>();
   // HashMap<Integer,ArrayList<Integer>> ProvValues = new HashMap<Integer, ArrayList<Integer>>();
     LinkedHashMap<Integer,LinkedHashMap<Integer,QuadPattern>> JoinPosVal = new LinkedHashMap<Integer,LinkedHashMap<Integer,QuadPattern>>();
    for (int i = 0; i < 3; i++) {//For each position
        
        //System.out.println("varsub:"+ varsubscript.get(i).StringQuadPatternPosition());
        
        if(varsubscript.get(i) != null){//we have variable in the position i
            if(MatchingPatterns[i] == 1){ //Copy 
               // System.out.println("mpika edo");
                qpid = (QuadPatternID)Utilities.get(0, spe.get(i));
                hashkey = Utilities.myHashFunction(qpid.getUnionNo(),qpid.getJoinNo());  
               
                    try {
                        TripleValues.put(i,findValueAttr(evaluationResults.get(hashkey), varsubscript.get(i).getIntPosition()));
                        QuadsIDs [i] = hashkey;
                        
                        
                    } catch (SQLException ex) {
                        System.out.println("Exception " + ex.getMessage() +" in insertToVirtuoso function!");
                    }
            }
            else{//Join case
                
                   
                    try {
                    JoinPosVal = findValuesJoin(spe.get(i), joinSubs.get(i),varsubscript.get(i));
                    TripleValues.put(i,findValueAttrJoin(JoinPosVal.get(varsubscript.get(i).getQuadPatternID().getJoinNo()),varsubscript.get(i).getIntPosition()));
                        System.out.println("vgika leo");
                    } catch (SQLException ex) {
                    Logger.getLogger(ImplementationTest2.class.getName()).log(Level.SEVERE, null, ex);
                }
           }
        }else{ //we have constant in the position i
             TripleValues.put(i,new ArrayList<String>());
             TripleValues.get(i).add(InsertClause[i]);
             QuadsIDs[i] = 0;
             //System.out.println("i: "+i +" "+InsertClause[i]+ " "+ProvValues.get(i));
            // Utilities.printSetString(TripleValues.get(i));
             
        }
   }
    
    if(QuadsIDs[0] == QuadsIDs[1] && QuadsIDs[0] != QuadsIDs[2]){ // subject and predicate are evaluated on the same quad pattern
        if(QuadsIDs[0]==0){
           
            return cartesianProductOnlyValues(varsubscript, TripleValues);
           
        }else{
            return cartesianProductTwoQpOnlyValues(varsubscript,TripleValues,2);
        }
    }
    else if (QuadsIDs[0] == QuadsIDs[2] && QuadsIDs[0] != QuadsIDs[1]){//subject and object are evaluated on the same quad pattern
        if(QuadsIDs[0]==0){
            return cartesianProductOnlyValues(varsubscript, TripleValues);
        }else{
            return cartesianProductTwoQpOnlyValues(varsubscript,TripleValues,1);
        }
    }
    else if (QuadsIDs[1] == QuadsIDs[2] && QuadsIDs[0] != QuadsIDs[1]) {//predicate and object are evaluated on the same quad pattern
        if(QuadsIDs[1]==0){
            return cartesianProductOnlyValues(varsubscript, TripleValues);
        }else{
        return cartesianProductTwoQpOnlyValues(varsubscript,TripleValues,0);
        }
    }
    else if (QuadsIDs[0] == QuadsIDs[1] && QuadsIDs[0] == QuadsIDs[2]){//where clause has one quad pattern
        if(QuadsIDs[0]==0){
            return cartesianProductOnlyValues(varsubscript, TripleValues);
        }else{
        return cartesianProductOneQpOnlyValues(varsubscript,TripleValues);
        }
    }
     //if(QuadsIDs[0] != QuadsIDs[1] && QuadsIDs[0] != QuadsIDs[2] && QuadsIDs[1] != QuadsIDs[2]){
        System.out.println("edo");
        return cartesianProductOnlyValues(varsubscript,TripleValues);
        
  
    
     
    /*for (int i = 0; i < Entries.size(); i++) {
        System.out.println("Entries size"+Entries.size());
        tmpQp = Entries.get(i).getQuadPattern();
        System.out.println("qp: "+tmpQp.getSubject()+" ,"+tmpQp.getPredicate()+" "+tmpQp.getObject());
        quad_ID = VirtuosoOperations.getQuadID(tmpQp.getSubject(),  tmpQp.getPredicate(), tmpQp.getObject(), InsertClause[3]);
        if (quad_ID == 0) {//This is the first time that the quadruple is inserted in Virtuoso
            VirtuosoOperations.executeSQLUpdate("insert into DB.DBA.MYQUADS (subject,predicate,object,graph) values ('"+ tmpQp.getSubject()+"','"+
             tmpQp.getPredicate()+"','"+tmpQp.getObject()+"','"+InsertClause[3]+"')");
         }
         else{
             cpe = VirtuosoOperations.getMaxCPENumber(quad_ID);
         }
      newID = VirtuosoOperations.getQuadID(Entries.get(i).getQuadPattern().getSubject(), Entries.get(i).getQuadPattern().getPredicate(), Entries.get(i).getQuadPattern().getObject(), InsertClause[3]);
       // System.out.println("prov: "+Entries.get(i).getProv().getProvPosition(0)+Entries.get(i).getProv().getProvPosition(1)+Entries.get(i).getProv().getProvPosition(2));
        VirtuosoOperations.executeSQLUpdate("insert into DB.DBA.MyProvenance (quad_ID, cpe, pe, prov_s, prov_p, prov_o) values ("+newID+","+(cpe+1)+","+(pe+1)+",'"+Entries.get(i).getProv().getProvPosition(0)+"','"+Entries.get(i).getProv().getProvPosition(1)+"','"+Entries.get(i).getProv().getProvPosition(2)+"')");
     
    }*/
   
   
}

/**
 * This function checks if the given Quad appears in the Quads table created through the previous graph patterns
 * @param currentGraphPattern 
 * @param tmpQp
 * @param AllEntries contains the individual Quads table created by the evaluation of each graph pattern
 * @return 
 */
 private static boolean checkPreviousGraphPatterns(int currentGraphPattern, QuadPattern tmpQp, HashMap<Integer,LinkedHashMap<Integer,VirtuosoEntry>> AllEntries) {
       for (int i = 1; i < currentGraphPattern; i++) {
           for (int j = 0; j < AllEntries.get(i).size(); j++) {
               if(tmpQp.compareQuadPatterns(AllEntries.get(i).get(j).getQuadPattern())){
                   return true;
               }
           }
     }
       return false;
    }
 
 /**
  * This function checks if the given Quad is already created from the evaluation of the currentGraphPattern
  * @param currentGraphPattern
  * @param tmpQp
  * @param AllEntries
  * @return 
  */
/* private static boolean checkCurrentGraphPattern(int currentGraphPattern, QuadPattern tmpQp, HashMap<Integer,LinkedHashMap<Integer,VirtuosoEntry>> AllEntries) {
       
           for (int j = 0; j < AllEntries.get(currentGraphPattern).size(); j++) {
               if(tmpQp.compareQuadPatterns(AllEntries.get(currentGraphPattern).get(j).getQuadPattern())){
                   return true;
               }
     }
       return false;
  }*/
 
  
/**
 * The provenance algorithm. Computes the provenance of the result quadruples of a SPARQL INSERT update
 * @param update a SPARQL INSERT Update following the syntax of our model
 **/
public static void provenanceConstruction(String update){
try{
    Iterator itr;
    HashMap<Integer,LinkedHashSet<QuadPatternID>> spe;
    //It is not a LinkedHashSet because we can have the same variable in two positions of the same quad
    ArrayList<QuadPatternPosition> varsub;
    //0 position represents the join subcripts in the subject position, etc in the ArrayList
    //In the LinkedHashMap each position represents a join subscript list, i.e. 0-> represents the first join subscript, etc
    //The inner ArrayList represents a set of QuadPattern positions, i.e., a join subscript
    joinSubs = new HashMap<Integer,ArrayList<ArrayList<QuadPatternPosition>>>();
    QuadPatternID qp_id;
    TreeSet<QuadPatternID> MP = new TreeSet<QuadPatternID>();
    int qp_insPos, i, currentGraphPattern, cpe = 1, pe = 1;
    //Integer shows the graphpatternNo
    HashMap<Integer,LinkedHashMap<Integer,VirtuosoEntry>> AllEntries = new HashMap<Integer, LinkedHashMap<Integer,VirtuosoEntry>>();
    int[] MatchingPatterns = new int[3];
 
    long startTime = System.currentTimeMillis();
    createComponents(update);

    /*Compute the provenance for each graph pattern*/
    for(currentGraphPattern = 1; currentGraphPattern<=graphPatternNo; currentGraphPattern++){// for all gp^i in WHERE clause
        //Initialization
        spe = new HashMap<Integer,LinkedHashSet<QuadPatternID>>();
        varsub = new ArrayList<QuadPatternPosition>();
        
       /*********    PE_COMPUTATION     *****************/
        for(qp_insPos = 0; qp_insPos<3; qp_insPos++){ // for all qp_ins.pos
           //  System.out.println("insCls: "+InsertClause[qp_insPos]);
        /*If the specific position contains a variable then it has to be processed*/
        if(InsertClause[qp_insPos].startsWith("?")){
            //Create the MatchingPatterns set
            MP = createMatchingPatterns(qp_insPos,InsertClause[qp_insPos], currentGraphPattern);
            //System.out.println("MPekso: "+MP.size());
            /*In this case no results can not be formed since a variable does not return mappings*/
            if(MP == null){
                break;
            }
            System.out.print("Start printing Matching Patterns set for variable "+InsertClause[qp_insPos]+": {");
            Utilities.printSetID(MP);
            System.out.println(" }");
             MatchingPatterns[qp_insPos]= MP.size();
                       
            itr = MP.iterator();
            while(itr.hasNext()){
                if(spe.get(qp_insPos) == null){
                    spe.put(qp_insPos,new LinkedHashSet<QuadPatternID>());
                    
                }
                
                qp_id = (QuadPatternID)itr.next();
                spe.get(qp_insPos).add(new QuadPatternID(qp_id.getUnionNo(), qp_id.getJoinNo()));
                
            }
           
            varsub.add(getVarSubscript(InsertClause[qp_insPos], currentGraphPattern));
            
            // Utilities.printSetID(spe.get(0));
             //System.out.println("edo");
             //Utilities.printSetID(spe.get(1));
             //System.out.println("edo1");
             //Utilities.printSetID(spe.get(2));


        }
        /*If the specific position contains a constant value then no evaluation is needed
         * and we assign prov_position to be 'Unknown'*/
        else{
            //Do Nothing...The function </code>insertToVirtuoso</code> will figure out and manipulate this case
            varsub.add(null);
            
        }

        
     }
        
        System.out.println("before entries");
        AllEntries.put(currentGraphPattern, insertToVirtuoso(MatchingPatterns,varsub,spe,joinSubs));

        System.out.println("after entries");
        
    
    }
    
    /**Insert to Virtuoso*/
    QuadPattern tmpQp;
    int quad_ID, newID;
    boolean exist;
    for (int j = 1; j <= graphPatternNo; j++) {
        for (int k = 0; k < AllEntries.get(j).size(); k++) {
            //System.out.println("mpika");
             tmpQp = AllEntries.get(j).get(k).getQuadPattern();
            // System.out.println("qp: "+tmpQp.getSubject()+" ,"+tmpQp.getPredicate()+" "+tmpQp.getObject());
             quad_ID = VirtuosoOperations.getQuadID(tmpQp.getSubject(),  tmpQp.getPredicate(), tmpQp.getObject(), InsertClause[3]);
             if (quad_ID == 0) {//This is the first time that the quadruple is inserted in Virtuoso
                 VirtuosoOperations.executeSQLUpdate("insert into DB.DBA.MYQUADS (subject,predicate,object,graph) values ('"+ tmpQp.getSubject()+"','"+
                 tmpQp.getPredicate()+"','"+tmpQp.getObject()+"','"+InsertClause[3]+"')");
                 cpe = 1;
                 pe = 1;
             }
             else{
                 cpe = VirtuosoOperations.getMaxCPENumber(quad_ID);
                 exist = checkPreviousGraphPatterns(j,tmpQp,AllEntries);
                // System.out.println("exist:"+exist+ "quad_id"+quad_ID);
               //  Utilities.printQuadPattern(tmpQp);
                 if(exist){
                     pe = VirtuosoOperations.getLastPENumber(quad_ID, cpe)+1;
                 }else{
                     //different ways to create the same quad through the same graph pattern
                    // if(checkCurrentGraphPattern(j, tmpQp, AllEntries)){
                         
                    // }else{
                         
                    // }
                     cpe++;
                     pe=1;
                 }
                
             }
      newID = VirtuosoOperations.getQuadID(AllEntries.get(j).get(k).getQuadPattern().getSubject(), AllEntries.get(j).get(k).getQuadPattern().getPredicate(),AllEntries.get(j).get(k).getQuadPattern().getObject(), InsertClause[3]);
        VirtuosoOperations.executeSQLUpdate("insert into DB.DBA.MyProvenance (quad_ID, cpe, pe, prov_s, prov_p, prov_o) values ("+newID+","+(cpe)+","+(pe)+",'"+AllEntries.get(j).get(k).getProv().getProvPosition(0)+"','"+AllEntries.get(j).get(k).getProv().getProvPosition(1)+"','"+AllEntries.get(j).get(k).getProv().getProvPosition(2)+"')");
     
    }
    }
    
    long endTime = System.currentTimeMillis();
    System.out.println("Update "+update+ " committed in " + (endTime - startTime) + " ms");
    
    
    }catch (Exception ex){
        System.out.println("Exception "+ ex.getMessage() + " occured during the execution of the update.");
    }
}
        

/**TO DO
 * This function executes a SPARQL INSERT after it is translated to SQL.
 * We follow the same process with the case that we want to compute provenance
 */
public static void runSPARQLUpdateNoProv(String upd) throws SQLException{
   Iterator itr;
    HashMap<Integer,LinkedHashSet<QuadPatternID>> spe;
    //It is not a LinkedHashSet because we  ArrayList<QuadPatternPosition> varsub;can have the same variable in two positions of the same quad
   ArrayList<QuadPatternPosition> varsub = new ArrayList<QuadPatternPosition>();
    //0 position represents the join subcripts in the subject position, etc in the ArrayList
    //In the LinkedHashMap each position represents a join subscript list, i.e. 0-> represents the first join subscript, etc
    //The inner ArrayList represents a set of QuadPattern positions, i.e., a join subscript
    joinSubs = new HashMap<Integer,ArrayList<ArrayList<QuadPatternPosition>>>();
    QuadPatternID qp_id;
    TreeSet<QuadPatternID> MP = new TreeSet<QuadPatternID>();
    int qp_insPos, i, currentGraphPattern, cpe = 1, pe = 1;
    //Integer shows the graphpatternNo
    HashMap<Integer,LinkedHashMap<Integer,QuadPattern>> AllEntries = new HashMap<Integer, LinkedHashMap<Integer,QuadPattern>>();
    int[] MatchingPatterns = new int[3];
    try{
    long startTime = System.currentTimeMillis();
    createComponents(upd);

    /*Compute the result for each graph pattern*/
    for(currentGraphPattern = 1; currentGraphPattern<=graphPatternNo; currentGraphPattern++){// for all gp^i in WHERE clause
        //Initialization
        spe = new HashMap<Integer,LinkedHashSet<QuadPatternID>>();
        
        
       /*********    Result_COMPUTATION     *****************/
        for(qp_insPos = 0; qp_insPos<3; qp_insPos++){ // for all qp_ins.pos
         //   System.out.println("ins: "+InsertClause[qp_insPos]);
        /*If the specific position contains a variable then it has to be processed*/
        if(InsertClause[qp_insPos].startsWith("?")){
            //Create the MatchingPatterns set
            MP = createMatchingPatterns(qp_insPos,InsertClause[qp_insPos], currentGraphPattern);
            /*In this case no results can not be formed since a variable does not return mappings*/
            if(MP == null){
                break;
            }
                                
            MatchingPatterns[qp_insPos] = MP.size();
            itr = MP.iterator();
            while(itr.hasNext()){
                if(spe.get(qp_insPos) == null){
                    spe.put(qp_insPos,new LinkedHashSet<QuadPatternID>());
                    
                }
                
                qp_id = (QuadPatternID)itr.next();
                spe.get(qp_insPos).add(new QuadPatternID(qp_id.getUnionNo(), qp_id.getJoinNo()));
                
            
            
                
            }
            varsub.add(getVarSubscript(InsertClause[qp_insPos], currentGraphPattern));

        }
        else{
            varsub.add(null);
        /*If the specific position contains a constant value then no evaluation is needed
         * and we assign prov_position to be 'Unknown'*/
        
        }
                
     }
        
       // System.out.println("before entries");
        //System.out.println("varsub: "+ varsub.get(0)+" "+varsub.get(1).StringQuadPatternPosition()+" "+varsub.get(2).StringQuadPatternPosition());
        AllEntries.put(currentGraphPattern, insertToVirtuosoWithoutProv(MatchingPatterns,varsub,spe,joinSubs));

        //System.out.println("after entries");
        
    
    }
    
    /**Insert to Virtuoso*/
    QuadPattern tmpQp;
    int quad_ID, newID;
    boolean exist;
    for (int j = 1; j <= graphPatternNo; j++) {
        for (int k = 0; k < AllEntries.get(j).size(); k++) {
           // System.out.println("mpika "+AllEntries.get(j).size());
             tmpQp = AllEntries.get(j).get(k);
          //  System.out.println("qp: "+tmpQp.getSubject()+" ,"+tmpQp.getPredicate()+" "+tmpQp.getObject());
             quad_ID = VirtuosoOperations.getQuadID(tmpQp.getSubject(),  tmpQp.getPredicate(), tmpQp.getObject(), InsertClause[3]);
             if (quad_ID == 0) {//This is the first time that the quadruple is inserted in Virtuoso
                 VirtuosoOperations.executeSQLUpdate("insert into DB.DBA.MYQUADS (subject,predicate,object,graph) values ('"+ tmpQp.getSubject()+"','"+
                 tmpQp.getPredicate()+"','"+tmpQp.getObject()+"','"+InsertClause[3]+"')");
                 cpe = 1;
                 pe = 1;
             }
             
                
            }
      
    }
    
    
    long endTime = System.currentTimeMillis();
    System.out.println("Update "+upd+ " committed in " + (endTime - startTime) + " ms");
    
    
    }catch (Exception ex){
        System.out.println("Exception "+ ex.getMessage() + " occured during the execution of the update.");
    }

    long endTime = System.currentTimeMillis();
   

}

 /**
  * This function executes a SPARQL INSERT update using Virtuoso features,
  * with no interior implmentation of the user
  * @param upd a SPARQL INSERT update
  */
public static void runSPARQLUpdateVirtuoso(String upd){
     long StartTime = System.nanoTime();
     
     VirtuosoOperations.executeSQLUpdate("sparql "+upd);
     long EndTime = System.nanoTime();
     System.out.println("SPARQL update "+ upd + " executed in "+(StartTime-EndTime)+" ns");
 }

/**
 * 
 * @param provPos
 * @param posNo
 */
public static void parseProvPos(String provPos, int posNo){
    int quad_id;
    int joinCounter = 0;
    QuadPatternPosition pos = new QuadPatternPosition();
    QuadPatternPosition varsubscript;
    if(provPos.matches("Unknown")){
        //System.out.println("edo"+provPos+posNo);
        RecVarSubs.put(posNo, null); //In this case varSub is null
        RecJoinSubs.put(posNo, null);
      //  System.out.println("red:"+RecVarSubs.get(posNo).StringQuadPatternPosition());
        return;
    }
    String spe = provPos.substring(6,provPos.length()-1);
    if(spe.contains("{")){ //Join
        //System.out.println("spe:"+spe);
        Matcher m = Pattern.compile("(\\d+[^{|}|)]*)").matcher(spe);
        RecJoinSubs.put(posNo,new ArrayList<ArrayList<QuadPatternPosition>>());
       // System.out.println("size:"+RecJoinSubs.size());
       
        while(m.find()){
           // System.out.println("find: "+m.group(0));
            if(!(m.group(0).contains("_"))){//It is a  quad ID
                quad_id = Integer.parseInt(m.group(0));
                RecQuadIDs.add(quad_id);
               
            }else{ //it is a join subscript
                RecJoinSubs.get(posNo).add(new ArrayList<QuadPatternPosition>());
                 for (String retval: m.group(0).split(",")){
                   //  System.out.println("retval: "+retval);
                    pos = Utilities.StringToQuadPattPos(retval);
                  //  System.out.println("joinCounter: "+(joinCounter));
                 //    System.out.println(""+RecJoinSubs.get(posNo).get(joinCounter)==null);
                    
                    RecJoinSubs.get(posNo).get(joinCounter).add(pos);
                
                 }
                 joinCounter++;
            }
           
        }
        
     } else { //Copy
         quad_id = Integer.parseInt(spe);
            RecQuadIDs.add(quad_id);
          //  System.out.println("quad_id "+quad_id);

    }
    //System.out.println("spe: "+spe);

    String varsub = provPos.substring(0,5); //varSub
    
    varsubscript = Utilities.StringToQuadPattPos(varsub);
    //System.out.println("varsub1: "+ varsub);
    RecVarSubs.put(posNo,new QuadPatternPosition(new QuadPatternID(varsubscript.getQuadPatternID().getUnionNo(), varsubscript.getQuadPatternID().getJoinNo()), varsubscript.getPosition()));
    //System.out.println("Varsub:"+RecVarSubs.get(posNo).getQuadPatternID().getUnionNo()); 
     
}

public static void createSubsPatterns(){
    QuadPatternPosition pos;
    QuadPatternID id;
    //Accessing all joinsubscripts
    for (int i = 0; i < RecJoinSubs.size(); i++) {
        if(RecJoinSubs.get(i) != null){
            for (int j = 0; j < RecJoinSubs.get(i).size(); j++) {
                for (int k = 0; k < RecJoinSubs.get(i).get(j).size(); k++) {
                    pos = RecJoinSubs.get(i).get(j).get(k);
                    id = new QuadPatternID(pos.getQuadPatternID().getUnionNo(), pos.getQuadPatternID().getJoinNo());
                    if(!Utilities.checkLHSetForDublicates(id, SubscriptPatterns)){
                    SubscriptPatterns.add(id);
                    
                   }
                }
                
            }
            
        }
    }
    //Accessing the varsubscripts
    for (int i = 0; i < RecVarSubs.size(); i++) {
        //System.out.println("RecVars: "+RecVarSubs.get(i).StringQuadPatternPosition());
        if(RecVarSubs.get(i)!= null){
            pos = RecVarSubs.get(i);
            id = new QuadPatternID(pos.getQuadPatternID().getUnionNo(), pos.getQuadPatternID().getJoinNo());
                    //if(!Utilities.checkLHSetForDublicates(id, SubscriptPatterns)){
                    SubscriptPatterns.add(id);
           // }
            
        }

    }
}

/**
 * This function reconstructs a compatible SPARQL INSERT update using a quadruple and a cpe expression from its provenance
 * @param quadID the id of the quadruple we want to use in the reconstruction process
 * @param cpe an int from 1 to MaxCPE that represents a part of the provenance(cpe) of the specified quadruple
 */
public static void updateReconstruction(int quadID, int cpe) throws SQLException{
    
    ResultSet pe_res,res;
    String prov_pos, RecUpd="INSERT ", whereCl="",value;
    Iterator itr, itr1;
    QuadPatternID tmp_qp;
    QuadPatternPosition tmp_pos = new QuadPatternPosition();
    QuadPatternPosition tmp_pos2;
    QuadPattern quad = VirtuosoOperations.getQuadfromID(quadID);

    int var_count = 0, hashkey,hashkey2,j,k,r = 0,position;
    long duration;
    long startTime = System.nanoTime();
    //Assign graph Value
    RecInsertClause[3] = quad.getGraph();
    //Assign random variables in INSERT clause
    for(var_count = 0; var_count<3; var_count++){
         RecInsertClause[var_count] = "?v"+Integer.toString(var_count);
    }
    
    int maxPE = VirtuosoOperations.getLastPENumber(quadID, cpe);
           
    for (int i = 1; i <= maxPE; i++) {
       RecVarSubs = new LinkedHashMap<Integer,QuadPatternPosition>();
       RecJoinSubs = new LinkedHashMap<Integer, ArrayList<ArrayList<QuadPatternPosition>>>();
       RecQuadIDs = new ArrayList<Integer>();
       SubscriptPatterns = new LinkedHashSet<QuadPatternID>();
       PeGraphs = new ArrayList<String>();
       RecUpdate = new LinkedHashMap<Integer, QuadPattern>();
       pe_res = VirtuosoOperations.executeSQLQuery("select prov_s,prov_p,prov_o from DB.DBA.MyProvenance where quad_ID = "+quadID+ " and cpe = "+cpe +" and pe = "+i);
       pe_res.next();
       for (j = 0; j < 3; j++) {
            prov_pos = pe_res.getString(j+1);
          //  System.out.println("prov:"+prov_pos);
            parseProvPos(prov_pos, j);
           // System.out.println("vgola");
         }
       
       itr1 = RecQuadIDs.iterator();
       
       while(itr1.hasNext()){//Construct PeGraphs Set
           res = VirtuosoOperations.executeSQLQuery("select graph from DB.DBA.MYQUADS where quad_ID = "+(Integer) itr1.next());
           res.next();
           PeGraphs.add(res.getString(1));
           System.out.println("pe: "+PeGraphs.get(PeGraphs.size()-1));
       }
       createSubsPatterns();
       //For each element of SubscriptsPatterns create a new QuadPattern
       itr = SubscriptPatterns.iterator();
       itr1 = PeGraphs.iterator();
      Iterator tr = SubscriptPatterns.iterator();
       QuadPatternID qp;
       while(tr.hasNext()){
           qp = (QuadPatternID)tr.next();
           System.out.println("Susvrpy: "+qp.getUnionNo()+" "+qp.getJoinNo() );
       }
       Iterator tr1 = PeGraphs.iterator();
       while(tr1.hasNext()){
           System.out.println("pegraphs: "+(String)tr1.next());
       }
       String test;
      // while(itr.hasNext() &&itr1.hasNext()){
      while(itr1.hasNext()){
           tmp_qp = (QuadPatternID) itr.next();
           test = (String)itr1.next();
           System.out.println("tmp:"+tmp_qp.getJoinNo()+" "+test);
           hashkey = Utilities.myHashFunction(tmp_qp.getUnionNo(), tmp_qp.getJoinNo());
           RecUpdate.put(hashkey, new QuadPattern(null,null,null,test));
           System.out.println("gia na do:"+RecUpdate.get(hashkey).getGraph());
        }

       //varsub.pos = qp'ins.pos
       for (j = 0; j < 3; j++) {
           if(RecVarSubs.get(j)!=null){
              // System.out.println("mpika"+j);
              // System.out.println("REC"+RecVarSubs.get(j)==null);
           tmp_pos = RecVarSubs.get(j);
     //          System.out.println("tmp+pos"+tmp_pos.StringQuadPatternPosition());
           if(tmp_pos.getQuadPatternID() != null){
                hashkey = Utilities.myHashFunction(tmp_pos.getQuadPatternID().getUnionNo(), tmp_pos.getQuadPatternID().getJoinNo());
              //  System.out.println("hasheky"+hashkey);
                RecUpdate.get(hashkey).setPositionValue(RecInsertClause[j], tmp_pos.getIntPosition());
             
           }
           }else{
             //  System.out.println("mpika"+j);
               RecInsertClause[j] = quad.getPositionValue(j); //gp'_ins.pos = q.pos
              // System.out.println("REC"+RecInsertClause[j]); 
           }
        }
        
       //Join
        for (j = 0; j < 3; j++) {//j->posNo
            //If the current position contains join subscripts
             if(RecJoinSubs.get(j) != null){
                 //while joinSub^r != null
                 for(r = 0; r<RecJoinSubs.get(j).size(); r++) {
                     //for all jp^r_k
                    for(k =0; k<RecJoinSubs.get(j).get(r).size(); k++){
                        tmp_pos = RecJoinSubs.get(j).get(r).get(k);
                        hashkey = Utilities.myHashFunction(tmp_pos.getQuadPatternID().getUnionNo(), tmp_pos.getQuadPatternID().getJoinNo());
                        if(RecUpdate.get(hashkey).getPositionValue(tmp_pos.getIntPosition())== null){
                           RecUpdate.get(hashkey).setPositionValue("?v"+var_count, tmp_pos.getIntPosition());
                           var_count++;
                        }
                        tmp_pos2 = RecJoinSubs.get(j).get(r+1).get(k);
                        System.out.println("tmp_pos2 "+tmp_pos2.StringQuadPatternPosition());
                        hashkey2 = Utilities.myHashFunction(tmp_pos2.getQuadPatternID().getUnionNo(), tmp_pos2.getQuadPatternID().getJoinNo());
                        value = RecUpdate.get(hashkey).getPositionValue(tmp_pos.getIntPosition());
                        System.out.println("value: "+value);
                        System.out.println("hashkey2: "+RecUpdate.get(hashkey2)==null);
                        System.out.println("pos: "+tmp_pos2.getPosition());
                        Utilities.printQuadPattern(RecUpdate.get(hashkey2));
                        RecUpdate.get(hashkey2).setPositionValue(value,tmp_pos2.getIntPosition());
                        k++;
                    }
                    r=r+1;
                 }
                
             }
        }
       
        for(Integer key: RecUpdate.keySet()){
           for(position = 0; position<3; position++){
               
                if(RecUpdate.get(key).getPositionValue(position) == null){
                    RecUpdate.get(key).setPositionValue("?v"+Integer.toString(var_count), position);
                    var_count++;
                }
                
            }
         }
       
        whereCl = whereCl +  Utilities.printMyHash(RecUpdate);
        if(i <maxPE){
            whereCl = whereCl + " UNION ";
        }
        
        }
    RecUpd =  RecUpd+"{ ("+RecInsertClause[0]+", "+RecInsertClause[1]+", "+RecInsertClause[2]+", "+RecInsertClause[3]+") } WHERE { " +whereCl+" }";
    long endTime = System.nanoTime();
    System.out.println("");
    duration = TimeUnit.NANOSECONDS.toMillis(endTime-startTime);
    System.out.println("duration "+(endTime - startTime)/1000);
    System.out.println("Update "+ RecUpd +"\n reconstructed in "+duration+" ms");
    
}

public static void main(String[] args) throws Exception {
        
        /*Connect to Virtuoso database*/
        VirtuosoOperations VirtSession = new VirtuosoOperations("dba","dba");

        /*The user selects if he wants to load an RDF file or make a SPARQL INSERT update*/
        Scanner input = new Scanner(System.in);
       
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(">>>>>>>>>>>>>>>> Welcome to the Provenance System! <<<<<<<<<<<<<<<<<<");
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println("1. Load quadruples from an N-Q file");
        System.out.println("2. Execute a SPARQL INSERT update");
        System.out.println("3. Reconstruct a SPARQL INSERT update using input from Virtuoso ");
        System.out.println("4. Exit");
        String option = input.next();
        if(option.matches("1")){ //Load quadruples from file
            System.out.println("Please insert the name of the file: ");
            //String filename = input.next();
        /*Import quads from a file data-0*/
       // Utilities.removeBlankNodes("btc-2009-small.nq","_:");
       // VirtuosoOperations.loadRDFFileToVirtuoso("NoBN_"+"data-1.nq",0);
        VirtuosoOperations.loadRDFFileToVirtuoso("NoBN_btc2009_small_500K.txt",0);
 
    //     VirtuosoOperations.loadRDFFileToVirtuoso("NoBN_taxonomy-citations.nq",0);//250k
   //      VirtuosoOperations.loadRDFFileToVirtuoso("Dataset_Bio2RDF_165K.txt",0); // 140k
  //         VirtuosoOperations.loadRDFFileToVirtuoso("Dataset_Bio2RDF_110K.txt",0); //95k
   //         VirtuosoOperations.loadRDFFileToVirtuoso("Dataset_Bio2RDF_60K.txt",0);//50k
//            VirtuosoOperations.loadRDFFileToVirtuoso("Dataset_Bio2RDF_13K.txt",0);//10k

        // VirtuosoOperations.loadRDFFileToVirtuoso("Dataset_Bio2RDF_10000.txt",0);
        }
        else if(option.matches("2")){//Insert an update
            String upd = Utilities.inputUpdate();
           provenanceConstruction(upd);
        //    runSPARQLUpdateNoProv(upd);
         /* System.out.println("Please select an option for the computation process: ");
            System.out.println("1. Compute Results with Provenance Info");
            System.out.println("2. Compute only Results");
            System.out.println("3. Compute Results with Virtuoso features ");
            int proc = input.nextInt();
            String upd = Utilities.inputUpdate();
            if(proc == 1){
                provenanceConstruction(upd);
            }else if(proc == 2){
                runSPARQLUpdateNoProv(upd);
            }else if(proc == 3){
                runSPARQLUpdateVirtuoso(upd);
            }
            else{
                System.out.println("This option is not available!");
                
            }*/
        } else if (option.matches("3")){
           System.out.println("Please insert a quadruple ID: ");
           int quadID = input.nextInt();
           int maxCPE = VirtuosoOperations.getMaxCPENumber(quadID);
           System.out.println("Please insert a cpe number from 1 to "+maxCPE);
           int cpe = input.nextInt();
           updateReconstruction(quadID, cpe);
        }
        else{//Exit the system
        System.out.println("Thank you for using PC system!");
        Timer a = new Timer();
        a.schedule(new TimerTask() {

           private int counter = 3;
           @Override
           public void run() {
               if(counter == 0) {
                   System.out.println("System exit...");
                   System.exit(0);
               }
               
               System.out.println("The system will terminate in "+counter--+" seconds...");
           }
       }, 0, 1000);
            
        }

       /*Close the connection to Virtuoso database*/
       VirtSession.terminate();

    }

   



}
