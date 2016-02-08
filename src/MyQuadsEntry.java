/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NewVersionProvenance;

import java.util.Comparator;

/**
 *
 * @author Argyro Avgoustaki
 * @email argiro@ics.forth.gr
 */
public class MyQuadsEntry implements Comparable<MyQuadsEntry>, Comparator<MyQuadsEntry>{
    private int  quad_id;
    private QuadPattern qp;
    public MyQuadsEntry(int quad_id, QuadPattern qp){
        this.qp = qp;
        this.quad_id = quad_id;
    }
    
    public QuadPattern getMyQuadsEntryPattern(){
        return this.qp;
    }
    
    public int getMyQuadsEntryID(){
        return this.quad_id;
    }
    
    public int compare(MyQuadsEntry qpe1, MyQuadsEntry qpe2) {
        Integer quad_id1 = (Integer)qpe1.getMyQuadsEntryID();
        Integer quad_id2 = (Integer)qpe2.getMyQuadsEntryID();
       return quad_id1.compareTo(quad_id2);
        
}

public int compareTo(MyQuadsEntry o) {
    return this.compare(this,o);
}

@Override
    public boolean equals(Object o) {
        return this.toString().equals(((MyQuadsEntry)o).toString());
    }
    
}
