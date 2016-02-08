/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package NewVersionProvenance;

import java.util.Comparator;

/**
 *
 * @author Argyro Avgoustaki
 * @email argiro@ics.forth.gr
 */
public class QuadPatternPosition implements Comparable<QuadPatternPosition>, Comparator<QuadPatternPosition>{
    
    private QuadPatternID identifier;
    private String pos;
    
    public QuadPatternPosition(QuadPatternID ID, String position){
        this.identifier = ID;
        this.pos = position;
    }
    
    public QuadPatternPosition(){
        this.identifier = null;
        this.pos = null;
    }
    
    public QuadPatternID getQuadPatternID(){
        return this.identifier;
    }
    
    public void setQuadpatternID(QuadPatternID ID){
        this.identifier = ID;
    }
    
    public String getPosition(){
        return this.pos;
    }

    public String getVirtuosoPosition(){
        if(this.pos.matches("s")){
            return "subject";
        }else if(this.pos.matches("p")){
            return "predicate";
        }else if(this.pos.matches("o")){
            return "object";
        }else if (this.pos.matches("g")){
            return "graph";
        }
       return "null";
    }
    
    public int getIntPosition(){
        if(this.pos.matches("s")){
            return 0;
        }else if(this.pos.matches("p")){
            return 1;
        }else if(this.pos.matches("o")){
            return 2;
        }else if (this.pos.matches("g")){
            return 3;
        }
       return -1;
    }
    
    /**
     * 
     * @param position 0 for subject, 1 for predicate, 2 for object
     */
    public void setPosition(String position){
        this.pos = position;
    }
    
/**
 * Gets the String representation of a </code>QuadPatternPosition</code>
 * @return qp^i_j.pos, where i is the union, j is the join and pos represents a position of the quadpattern
 */
public String StringQuadPatternPosition(){
   // return "qp^"+this.getQuadPatternID().getUnionNo()+"_"+this.getQuadPatternID().getJoinNo()+"."+this.pos;
    return this.getQuadPatternID().getUnionNo()+"_"+this.getQuadPatternID().getJoinNo()+"."+this.pos;
}

/**
* Compares its two arguments.  Returns a negative integer, zero, or a positive integer
* as the first argument is less than, equal to, or greater than the second.<p>
*
* @param qp1 the first object to be compared.
* @param qp2 the second object to be compared.
* @return a negative integer, zero, or a positive integer if the first argument is less than,
* equal to, or greater than the second.
*/
public int compare(QuadPatternPosition qp1, QuadPatternPosition qp2) {
        Integer union1 = (Integer)qp1.getQuadPatternID().getUnionNo();
        Integer union2 = (Integer)qp2.getQuadPatternID().getUnionNo();
        int CompareUnion = union1.compareTo(union2);
        if(CompareUnion == 0){
            Integer join1 = (Integer)qp1.getQuadPatternID().getJoinNo();
            Integer join2 = (Integer)qp2.getQuadPatternID().getJoinNo();
            return join1.compareTo(join2);
        }else{
            return CompareUnion;
        }
}

/**
 * Compares this object with the specified object.  Returns a
 * negative integer, zero, or a positive integer as this object is less
 * than, equal to, or greater than the specified object.
 * @param o the object to be compared
 * @return a negative integer, zero, or a positive integer as this object
 * is less than, equal to, or greater than the specified object.
 */
public int compareTo(QuadPatternPosition o) {
    return this.compare(this,o);
}
            
}
