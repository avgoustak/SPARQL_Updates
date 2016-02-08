/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package NewVersionProvenance;

import java.util.Comparator;

/**
 *
 * @author Roula
 */
public class QuadPatternID implements Comparable<QuadPatternID>, Comparator<QuadPatternID>{

private int unionNo, joinNo;
    
    public QuadPatternID(int unionNo, int joinNo){
        this.unionNo = unionNo;
        this.joinNo = joinNo;
    }
    
    public QuadPatternID(){
        this.joinNo = 0;
        this.unionNo = 0;
    }    
    
    public int getUnionNo(){
        return this.unionNo;
    }
    
    public void setUnionNo(int unionNo){
        this.unionNo = unionNo;
    }
    
    public int getJoinNo(){
        return this.joinNo;
    }
    
    public void setJoinNo(int joinNo){
        this.joinNo = joinNo;
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
public int compare(QuadPatternID qp1, QuadPatternID qp2) {
        Integer union1 = (Integer)qp1.getUnionNo();
        Integer union2 = (Integer)qp2.getUnionNo();
        int CompareUnion = union1.compareTo(union2);
        if(CompareUnion == 0){
            Integer join1 = (Integer)qp1.getJoinNo();
            Integer join2 = (Integer)qp2.getJoinNo();
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
public int compareTo(QuadPatternID o) {
    return this.compare(this,o);
}
}
