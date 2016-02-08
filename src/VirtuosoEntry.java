/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NewVersionProvenance;

/**
 *
 * @author Argyro Avgustaki
 * @email argiro@ics.forth.gr
 */
public class VirtuosoEntry {
    private QuadPattern qp;
    private Provenance prov;
    
     public VirtuosoEntry(QuadPattern QP, Provenance Prov){
        this.qp = QP;
        this.prov = Prov;
    }  
     
     
     public QuadPattern getQuadPattern(){
         return this.qp;
     }
     
     public Provenance getProv(){
         return this.prov;
     }
     
     public void setQuadPattern(QuadPattern qp){
         this.qp = qp;
     }
     
     public void setProv(Provenance prov){
         this.prov = prov;
     }
}
