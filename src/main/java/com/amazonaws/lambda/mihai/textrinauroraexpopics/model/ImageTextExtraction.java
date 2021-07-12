package com.amazonaws.lambda.mihai.textrinauroraexpopics.model;

import java.util.List;

import com.amazonaws.services.textract.model.Block;

/**
 * class used to encapsulate Textract results in order to serialize to JSON by JAKSON framework
 * @author Mihai ADAM
 *
 */
public class ImageTextExtraction {
	
	private String fileName;
	private String exibition;
	private List<Block> extractedTexts;

	public ImageTextExtraction() {
		
	}

	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public List<Block> getExtractedTexts() {
		return extractedTexts;
	}
	public void setExtractedTexts(List<Block> extractedTexts) {
		this.extractedTexts = extractedTexts;
	}
	public String getExibition() {
		return exibition;
	}
	public void setExibition(String exibition) {
		this.exibition = exibition;
	}


}