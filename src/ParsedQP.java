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
public class ParsedQP{
    public QuadPatternID identifier;
    public QuadPattern qp;
    
    public ParsedQP(QuadPatternID qpID, QuadPattern qp){
        this.identifier = qpID;
        this.qp = qp;            
    }
    
    public ParsedQP(){
        this.identifier = null;
        this.qp = null; 
    }
    
    public QuadPatternID getQPIdentifier(){
        return this.identifier;
    }   
    
    public void setQPIdentifier(QuadPatternID ID){
        this.identifier = ID;
    }
    
    public QuadPattern getQuadPattern(){
        return this.qp;
    }
    
    public void setQuadPattern(QuadPattern qp){
        this.qp = qp;
    }
    
        
    
}
