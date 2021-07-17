package com.amazonaws.lambda.mihai.textrinauroraexpopics.model;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * class that prepare&hold data from the file name for relational database 
 * passed from business logic to data access layer
 * @author Mihai ADAM
 *
 */
public class PageBuilder {

	private final String s3Key;//S3 Key: romexpo/2019.01.01_diverse/book-1_covf-1_pagn-120_typec-mag.jpg
	
	private final String[] s3KeyParts; //romexpo  2019.01.01_diverse  book-1_covf-1_pagn-120_typec-mag.jpg
	
	private static final String s3KEY_SEPARATOR = "/";
	private final SimpleDateFormat dateFormatterCode = new SimpleDateFormat("yyyyMMdd");
	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern( "yyyy.MM.dd" );
	
	private String expoLocation;
	
	private String expoName;
	private String expoDateStr;
	private Date expoDate;
	private String expoCode;
	
	private String docFormat;
	private Integer docCode;
	
	private String pageType;
	private Integer pageNumber;
	private Integer pageSection;// this is not final in case of Visiting Card where are more in one scan => are split
		
	private Integer docNumPages;// this is not final in case of Visiting Card where this is always set to 1
	private String docCompany;
	private List<String> companyAssociations;
	
	private String docContentType;
	
	private String s3Bucket;

	private Scan scan;
	
	// taken from database
	private Integer expoId;
	private Integer documentId;
	private Integer companyId;
	private Integer pageId;
	
	public PageBuilder (String s3Key) {
				
		this.s3Key = s3Key;
		s3KeyParts =  s3Key.split(s3KEY_SEPARATOR);
		
		Pattern itemCode = Pattern.compile("^[a-z]+$");
				
		expoLocation = s3KeyParts[0];
		if (!itemCode.matcher(expoLocation).matches()) {
			throw new RuntimeException("Malformed key name (location name) for key "+ s3Key);
		}
		
		expoName = s3KeyParts[s3KeyParts.length - 2].split("_")[1];
		expoDateStr = s3KeyParts[s3KeyParts.length - 2].split("_")[0];
		
		if (!itemCode.matcher(expoName).matches()) {
			throw new RuntimeException("Malformed key name (expo name) for key "+ s3Key);
		}
		try {
			
			//expoDate = dateFormatter.parse(expoDateStr);
			LocalDate ld = LocalDate.parse( expoDateStr , dateFormatter );
			expoDate = Date.from(ld.atStartOfDay()
				      .atZone(ZoneId.systemDefault()) 
				      .toInstant());
			
		} catch (DateTimeParseException e) {
			e.printStackTrace();
			throw new RuntimeException("Malformed key name (expo date) for key "+ s3Key, e);
		}
				
		String fileName = s3KeyParts[s3KeyParts.length - 1].substring(0, s3KeyParts[s3KeyParts.length - 1].lastIndexOf('.'));
		String[] fileNameParts = fileName.split("_");
		
		if (fileNameParts.length < 3 || fileNameParts.length > 6) {
			throw new RuntimeException("Malformed file name (unexpected parts number) for key "+ s3Key);
		}
		
		Pattern fileAllParts = Pattern.compile("^[0-9]{9}_[a-z]+-[0-9]+_[a-z]+-[0-9]+(?:-[0-9]+)?(?:_pagn-[0-9]+)?(?:_co-[a-z0-9,;]+)?(?:_typec-[a-z]+)?$");
		
		if (!fileAllParts.matcher(fileName).matches()) {
			throw new RuntimeException("Malformed file name for key "+ s3Key);
		}
		
		setCommonParts(fileNameParts);
		
		if ("covf".equals(pageType) && Integer.valueOf(1).equals(pageNumber)) {
						
			setCoverFaceSpecialParts(fileNameParts);
			
			if (docNumPages == null || docCompany == null) {
				throw new RuntimeException("Malformed file name (unexpected FRONT COVER parts number) for key "+ s3Key);
			}
			
		} else {
			if (fileNameParts.length > 3) {
				throw new RuntimeException("Malformed file name (unexpected parts number: > 3) for key "+ s3Key);
			}
		}
		
		if (expoCode == null || docFormat == null || docCode == null || pageType == null || pageNumber == null) {
			throw new RuntimeException("Malformed file name (unexpected COMMON parts number) for key "+ s3Key);
		}
	}
	
	/**
	 * set file name parts that exists for all files
	 * @param fileNameParts
	 */
	private void setCommonParts(String[] fileNameParts) {
		
		expoCode = fileNameParts[0];
		
		if (!dateFormatterCode.format(expoDate).equals(expoCode.substring(0, 8))) {
			throw new RuntimeException("Malformed file name (unexpected expo code) for key "+ s3Key);
		}
		
		docFormat = fileNameParts[1].split("-")[0];
		docCode = new Integer(fileNameParts[1].split("-")[1]);
		
		pageType = fileNameParts[2].split("-")[0];
		pageNumber = new Integer(fileNameParts[2].split("-")[1]);
		pageSection = (fileNameParts[2].split("-").length > 2) ? new Integer(fileNameParts[2].split("-")[2]) : null;
	}
	
	/**
	 * set file name parts that exists only for front cover
	 * @param fileNameParts
	 */
	private void setCoverFaceSpecialParts(String[] fileNameParts) {
		 
		if (!"visit".equals(docFormat) && !(fileNameParts.length > 4)) {
			throw new RuntimeException("Malformed file name (unexpected parts number: <= 4) for key "+ s3Key);
		}
		
		if (fileNameParts.length > 3) {
			
			docNumPages = new Integer(fileNameParts[3].split("-")[1]);
			
		}
		
		if (fileNameParts.length > 4) {
			
			String docCompanies = fileNameParts[4].split("-")[1];
			
			if (docCompanies.contains(";")) {
				String[] companies = docCompanies.split(";");
				docCompany = companies[0];
				companyAssociations = Arrays.asList(Arrays.copyOfRange(companies, 1, companies.length));
				
			} else {
				docCompany = docCompanies;
			}
			
		}
		
		if (fileNameParts.length > 5) {
			docContentType = fileNameParts[5].split("-")[1];
		}
	}

	
	public void setPageSection(Integer pageSection) {
		this.pageSection = pageSection;
	}



	public void setDocNumPages(Integer docNumPages) {
		this.docNumPages = docNumPages;
	}



	public static String getExpoLocation (String s3Key) {
		String[] s3KeyParts =  s3Key.split(s3KEY_SEPARATOR);
		String expoLocation = s3KeyParts[0];
		return expoLocation;
	}
	
	public String toString() {
		String lineSeparator = System.getProperty("line.separator");
		StringBuffer sb = new StringBuffer();
		
		sb.append("s3Key: " + getS3Key()).append(lineSeparator);
		sb.append("expoLocation: " + getExpoLocation()).append(lineSeparator);
		sb.append("expoKey: " + getExpoKey()).append(lineSeparator);
		sb.append("expoName: " + getExpoName()).append(lineSeparator);
		sb.append("expoDateStr: " + expoDateStr).append(lineSeparator);
		sb.append("expoDate: " + getExpoDate()).append(lineSeparator);
		sb.append("g1 expoCode: " + getExpoCode()).append(lineSeparator);
		sb.append("g2 docFormat: " + getDocFormat()).append(lineSeparator);
		sb.append("g2 docCode: " + getDocCode()).append(lineSeparator);
		sb.append("g3 pageType: " + getPageType()).append(lineSeparator);
		sb.append("g3 pageNumber: " + getPageNumber()).append(lineSeparator);
		sb.append("g3 pageSection: " + getPageSection()).append(lineSeparator);
		sb.append("g4 docNumPages: " + getDocNumPages()).append(lineSeparator);
		sb.append("g4 docCompany: " + getDocCompany()).append(lineSeparator);
		sb.append("g4 companyAssociations: " + Arrays.toString(getCompanyAssociations().toArray())).append(lineSeparator);
		sb.append("g5 docContentType: " + getDocContentType()).append(lineSeparator);
		
		return sb.toString();
	}
	
	
	
	public List<String> getCompanyAssociations() {
		return companyAssociations;
	}

	public Scan getScan() {
		return scan;
	}

	public void setScan(Scan scan) {
		this.scan = scan;
	}

	public String getExpoLocation() {
		return expoLocation;
	}

	public String getExpoKey() {
		return expoDateStr + s3KEY_SEPARATOR + getExpoName();
	}
	
	
	public void setS3Bucket(String s3Bucket) {
		this.s3Bucket = s3Bucket;
	}
	
	public String getScanS3Key() {
		return s3Key;
	}

	
	public Date getExpoDate() {
		return expoDate;
	}

	
	public String getS3Key() {
		return s3Key;
	}

	public String getExpoName() {
		return expoName;
	}

	public String getS3Bucket() {
		return s3Bucket;
	}

	public String getDocFormat() {
		return docFormat;
	}

	public Integer getDocCode() {
		return docCode;
	}

	public String getPageType() {
		return pageType;
	}

	public Integer getPageNumber() {
		return pageNumber;
	}

	public Integer getPageSection() {
		return pageSection;
	}

	public Integer getDocNumPages() {
		return docNumPages;
	}

	public String getDocContentType() {
		return docContentType;
	}

	public String getScanS3URL() {
		return (scan == null)? null : scan.getScanS3URL();
	}


	public String getS3ObjArn() {
		return (scan == null)? null : scan.getS3ObjArn();
	}


	public String getS3ObjEtag() {
		return (scan == null)? null : scan.getS3ObjEtag();
	}


	public String getScanFilePath() {
		return (scan == null)? null : scan.getScanFilePath();
	}


	public String getJsonMlFilePath() {
		return (scan == null)? null : scan.getJsonMlFilePath();
	}

	public String getExpoCode() {
		return expoCode;
	}

	public String getDocCompany() {
		return docCompany;
	}


	public Integer getExpoId() {
		return expoId;
	}


	public void setExpoId(Integer expoId) {
		this.expoId = expoId;
	}


	public Integer getDocumentId() {
		return documentId;
	}


	public void setDocumentId(Integer documentId) {
		this.documentId = documentId;
	}


	public Integer getCompanyId() {
		return companyId;
	}


	public void setCompanyId(Integer companyId) {
		this.companyId = companyId;
	}


	public Integer getPageId() {
		return pageId;
	}


	public void setPageId(Integer pageId) {
		this.pageId = pageId;
	}
	
	
}
