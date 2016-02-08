/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package NewVersionProvenance;

/**
 *
 * @author Argyro Avgoustaki
 * @email argiro@ics.forth.gr
 */
public class VisitedQuadPatternPos {
private QuadPatternPosition pos;
private int visited = 0;

public VisitedQuadPatternPos(QuadPatternPosition pos, int visited){
        this.pos = pos;
        this.visited = visited;
}

public void setVisitedQuadPattern(QuadPatternPosition pos){
    this.pos = pos;
}

public void setVisited(int visited){
    this.visited = visited;
}

public int getVisited(){
    return this.visited;
}

}
