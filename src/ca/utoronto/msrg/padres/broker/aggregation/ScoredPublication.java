package ca.utoronto.msrg.padres.broker.aggregation;

import ca.utoronto.msrg.padres.common.message.Publication;

public class ScoredPublication {

	Publication publication;
	int score;
	public ScoredPublication(Publication publication2, int score2) {
		this.publication=publication2;
		this.score=score2;
	}
	public Publication getPublication() {
		return publication;
	}
	public void setPublication(Publication publication) {
		this.publication = publication;
	}
	public int getScore() {
		return score;
	}
	public void setScore(int score) {
		this.score = score;
	}
	
	
	
	
}
