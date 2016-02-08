/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package NewVersionProvenance;

/**
 *
 * @author argiro
 */
public class Provenance {
    
private  String prov_s, prov_p, prov_o;
private int quad_ID, cpe, pe;

    public Provenance(int quad_ID, int cpe, int pe, String prov_s, String prov_p, String prov_o){
        this.quad_ID = quad_ID;
        this.cpe = cpe;
        this.pe = pe;
        this.prov_s = prov_s;
        this.prov_p = prov_p;
        this.prov_o = prov_o;

    
    }

    public Provenance(String prov_s, String prov_p, String prov_o){
        this.prov_s = prov_s;
        this.prov_p = prov_p;
        this.prov_o = prov_o;
    }
    
    public Provenance(){
        this.prov_s = null;
        this.prov_p = null;
        this.prov_o = null;
    }
    
    public void setQuadID(int quadID){
        if(quadID <= 0){
            System.out.println("Negative parameter in setQuadID function!");
            return;
        }
        this.quad_ID = quadID;
    }

    public void setCpe(int cpe){
        if(cpe <= 0){
            System.out.println("Negative parameter in setCpe function!");
            return;
        }
        this.cpe = cpe;
    }

    public void setPe(int pe){
        if(pe <= 0){
            System.out.println("Negative parameter in setPe function!");
            return;
        }
    }

    public void setProvPosition(String prov, int pos){
        if(prov == null || pos <=0){
            System.out.println("Wrong parameter in setProvPosition function!");
            return;
        }

        switch(pos){
            case 0:
                    this.prov_s = prov;
                    break;
            case 1:
                    this.prov_p = prov;
                    break;
            case 2:
                    this.prov_o = prov;
                    break;
            default:
                break;
        }

    }

    public void setProvPosition(String prov, String pos){
        if(prov == null || pos == null){
            System.out.println("Null parameter in setProvPosition function!");
            return;
        }
        if(pos.compareTo("s") == 0){
            this.prov_s = prov;
        }else if(pos.compareTo("p")== 0){
            this.prov_p = prov;
        }else if(pos.compareTo("o") == 0){
            this.prov_o = prov;
        }
    }

    public int getQuadID(){
        return this.quad_ID;
    }

    public int getCpe(){
        return this.cpe;
    }

    public int getPe(){
        return this.pe;
    }

    public String getProvPosition(int pos){
        switch(pos){
            case 0:
                 return this.prov_s;
                    
            case 1:
                 return this.prov_p;
                   
            case 2:
                 return this.prov_o;
                    
            default:
                return null;
                
        }
    }
}
