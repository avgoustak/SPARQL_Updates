/*
 * This class represents a quad pattern in the form qp :(s,p,o,g)
 */
package NewVersionProvenance;

import java.util.Comparator;

/**
 *
 * @author Roula
 */
public class QuadPattern {
  private  String s, p, o, g;
    
     public QuadPattern(String subject, String predicate, String object, String graph){
        this.s = subject;
        this.p = predicate;
        this.o = object;
        this.g = graph;            
    }
     
    public QuadPattern(){
        this.s = null;
        this.p = null;
        this.o = null;
        this.g = null;
    }
     
    public String getSubject(){
        return this.s;
    }
    
    public void setSubject(String subject){
        this.s = subject;
    }
    
    public String getPredicate(){
        return this.p;
    }
    
    public void setPredicate(String predicate){
        this.p = predicate;
    }
    
    public String getObject(){
        return this.o;
    }
    
    public void setObject(String object){
        this.o = object;
    }
    
    public String getGraph(){
        return this.g;
    }
    
    public void setGraph(String graph){
        this.g = graph;
    }
    
    public void setPositionValue(String value, int position){
        switch (position){
            case 0:
                this.s = value;
                break;
            case 1:
                this.p = value;
                break;
            case 2:
                this.o = value;
                break;
            default:
                break;
        }
    }

/**
 * Find the value of a position in a quad pattern
 * @param position: an integer where 0 refers to subject, 1 refers to predicate, 2 refers to object
 * and 3 refers to graph.
 * @return the String value of the position
 */
    public String getPositionValue(int position){
        String value;
        switch (position){
            case 0:
                value = getSubject();
                break;
            case 1:
                value = getPredicate();
                break;
            case 2:
                value = getObject();
                break;
            default:
                value = getGraph();
                break;
        }
        return value;
    }
    
    public boolean compareQuadPatterns(QuadPattern qp){
        if(this.s.compareTo(qp.s)==0){
            if(this.p.compareTo(qp.p)==0){
                if(this.o.compareTo(qp.o)==0){
                    return true;
                }
            }
            
        }
    return false;
   }
}
