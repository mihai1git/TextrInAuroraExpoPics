package com.amazonaws.lambda.mihai.textrinauroraexpopics.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.PageBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.textract.model.Block;

/**
 * layer between lambda logic and AURORA database
 * @author Mihai ADAM
 *
 */
public class AuroraDao {
	
	//database coordinates are in @DataSource: @DataSource.AURORA_URL

	// context injected from Cloud environment
    private Context handlerContext;

    /**
     * 
     * @param pageData data extracted from file name
     * @param mlResult data extracted by Textract from file content
     */
    public void writeToAurora (PageBuilder pageData, List<Block> mlResult) {

    	handlerContext.getLogger().log("START writing to database !");
    	    	
    	Connection conn = null;
        Statement statement = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
    	
        
    	try {
    		conn = (new DataSource()).getConnection();
    		handlerContext.getLogger().log("connection : " + conn);
    		conn.setAutoCommit(false); 
    		statement = conn.createStatement();
    		
    		Integer expoId = writeToAuroraExpo (pageData, mlResult, conn);
    		pageData.setExpoId(expoId);
    		
    		Integer companyId = writeToAuroraCompany (pageData, mlResult, conn);
    		pageData.setCompanyId(companyId);
    		    		
    		Integer documentId = writeToAuroraDocument (pageData, mlResult, conn); 
    		pageData.setDocumentId(documentId);
    		 
    		Integer scanId = writeToAuroraScan (pageData, mlResult, conn); 
    		
    		Integer pageId = writeToAuroraPage (pageData, mlResult, conn);
    		pageData.setPageId(pageId);
    		
    		Integer linesCount = writeToAuroraPageLines (pageData, mlResult, conn);
    		 
             conn.commit();
             
    	} catch (Exception e) {
    		try {
    			conn.rollback();
    		} catch (SQLException ex) {
	    		e.printStackTrace();
	            throw new RuntimeException(ex);
	    	}
    		
    		e.printStackTrace();
            throw new RuntimeException(e);
              
    	} finally {
    		try {
    			conn.close();
    		} catch (SQLException e) {
	    		e.printStackTrace();
	            throw new RuntimeException(e);
	    	}
    		
    	}
    	
    }
    
    private Integer writeToAuroraExpo (PageBuilder pageData, List<Block> mlResult, Connection conn) throws SQLException {
    	
    	Integer expoId = null;
    	
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
    	preparedStatement = conn.prepareStatement("select EXPO_ID from tab_expos where EXPO_S3_KEY = ?");
		preparedStatement.setString(1, pageData.getExpoKey());
		resultSet = preparedStatement.executeQuery();
		if (resultSet.next()) {
			expoId = resultSet.getInt(1);
			handlerContext.getLogger().log("existing expoId: " + expoId);
			
		} else {
    		preparedStatement = conn.prepareStatement("insert into tab_expos (EXPO_DATE, EXPO_NAME, EXPO_S3_KEY, EXPO_LOCATION_ID, EXPO_CODE) values (?, ?, ?, "
    		 		+ "(select LOCATION_ID from tab_expo_locations where LOCATION_CODE = ?), ?)", Statement.RETURN_GENERATED_KEYS);
    		   // Parameters start with 1
    		 preparedStatement.setDate(1, new java.sql.Date (pageData.getExpoDate().getTime()));
    		 preparedStatement.setString(2, pageData.getExpoName());
    		 preparedStatement.setString(3, pageData.getExpoKey());
    		 preparedStatement.setString(4, pageData.getExpoLocation());
    		 preparedStatement.setString(5, pageData.getExpoCode());
    		 preparedStatement.executeUpdate(); 

    		 resultSet = preparedStatement.getGeneratedKeys();
    		 if (resultSet.next()) {
    			 expoId = resultSet.getInt(1);
    			 handlerContext.getLogger().log("expoId: " + expoId);
    		 }
		}
		
		return expoId;
    }
    
    private Integer writeToAuroraCompany (PageBuilder pageData, List<Block> mlResult, Connection conn) throws SQLException {
    	
    	Integer companyId = null;
    	
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Statement statement = conn.createStatement();
        
        if (pageData.getDocCompany() != null) {//only if cover face page
			
			String query = getQueryToCheckEqualCompany(pageData.getDocCompany());
    		handlerContext.getLogger().log("query: " + query);
    		resultSet = statement.executeQuery(query);
    		
    		if (resultSet.next()) {
    			companyId = resultSet.getInt(1);
    			handlerContext.getLogger().log("existing companyId: " + companyId);
    			
    		} else {
			
    			preparedStatement = conn.prepareStatement("insert into tab_companies (COMPANY_NAME_CODES) values (?)", Statement.RETURN_GENERATED_KEYS);
    			preparedStatement.setString(1, pageData.getDocCompany());
        		preparedStatement.executeUpdate(); 
        		 
        		 resultSet = preparedStatement.getGeneratedKeys();
        		 if (resultSet.next()) {
        			 companyId = resultSet.getInt(1);
        			 handlerContext.getLogger().log("companyId: " + companyId);
        		 }
    		}
		}
		
		if (pageData.getCompanyAssociations() != null) {
			Integer assocCompanyId = null;
			for (String companyAssoc : pageData.getCompanyAssociations()) {
    			
				String query = getQueryToCheckLikeCompany(companyAssoc);
	    		handlerContext.getLogger().log("query: " + query);
	    		resultSet = statement.executeQuery(query);
	    		
	    		if (resultSet.next()) {
	    			assocCompanyId = resultSet.getInt(1);
	    			handlerContext.getLogger().log("associated existing companyId: " + assocCompanyId);
	    			
	    		} else {
				
    				preparedStatement = conn.prepareStatement("insert into tab_companies (COMPANY_NAME_CODES) values (?)");
    				preparedStatement.setString(1, companyAssoc);
    	    		preparedStatement.executeUpdate();
	    		}
			}
		}
		
		return companyId;
    }

	private Integer writeToAuroraDocument (PageBuilder pageData, List<Block> mlResult, Connection conn) throws SQLException {
		
    	Integer docId = null;
    	
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Statement statement = conn.createStatement();
		
		preparedStatement = conn.prepareStatement("select DOC_ID, DOC_NUM_PAGES, FMT_CODE from tab_documents docs, tab_doc_format_types dtypes where docs.DOC_FORMAT_ID = dtypes.FMT_ID and EXPO_ID = ? and DOC_CODE = ?");
		preparedStatement.setInt(1, pageData.getExpoId());
		preparedStatement.setInt(2, pageData.getDocCode());
		resultSet = preparedStatement.executeQuery();
		if (resultSet.next()) {
			docId = resultSet.getInt(1);
			Integer docNumPages = resultSet.getInt(2);
			String docFormatCode = resultSet.getString(3);
			handlerContext.getLogger().log("existing docId: " + docId + " with docNumPages: " + docNumPages + " and docFormatCode: " + docFormatCode);
			
			//check same type; if not same ERROR
			if (!docFormatCode.equals(pageData.getDocFormat())) {
				throw new RuntimeException("EXCEPTION: new page from other document, same doc code but different doc type; For one exibition doc code is from ONE sequence, not for each type");
			}
			
			
			//if cover face page not first, update info when that page found
			if (Integer.valueOf(0).equals(docNumPages) && DBConstants.PAGE_TYPE_COVF.equals(pageData.getPageType()) && Integer.valueOf(1).equals(pageData.getPageNumber())) {
				preparedStatement = conn.prepareStatement("update tab_documents set DOC_NUM_PAGES = ?, COMPANY_ID = ? where DOC_ID = ?");
				preparedStatement.setInt(1, pageData.getDocNumPages());
				preparedStatement.setInt(2, pageData.getCompanyId());
				preparedStatement.setInt(3, docId);
				preparedStatement.executeUpdate();
				
				if (pageData.getDocContentType() != null) {
					preparedStatement = conn.prepareStatement("update tab_documents set DOC_CONTENT_ID = (select CONTENT_ID from tab_doc_content_types where CONTENT_CODE = ?) where DOC_ID = ?");
					preparedStatement.setString(1, pageData.getDocContentType());
					preparedStatement.setInt(2, docId);
					preparedStatement.executeUpdate();
				}
				
			}
			
			//TODO check num pages
						
			
		} else {
			preparedStatement = conn.prepareStatement("insert into  tab_documents (EXPO_ID, DOC_CODE, DOC_FORMAT_ID, DOC_CONTENT_ID, DOC_NUM_PAGES, COMPANY_ID) "
    		 		+ "values (?, ?, (select FMT_ID from tab_doc_format_types where FMT_CODE = ?), (select CONTENT_ID from tab_doc_content_types where CONTENT_CODE = ?), ?, ?)", Statement.RETURN_GENERATED_KEYS);
    		 preparedStatement.setInt(1, pageData.getExpoId());
    		 preparedStatement.setInt(2, pageData.getDocCode());
    		 preparedStatement.setString(3, pageData.getDocFormat());
    		 if (pageData.getDocContentType() == null) preparedStatement.setNull(4, Types.VARCHAR);
    		 else preparedStatement.setString(4, pageData.getDocContentType());
    		 if (pageData.getDocNumPages() == null) preparedStatement.setNull(5, Types.INTEGER);
    		 else preparedStatement.setInt(5, pageData.getDocNumPages());
    		 if (pageData.getCompanyId() == null) preparedStatement.setNull(6, Types.INTEGER);
    		 else preparedStatement.setInt(6, pageData.getCompanyId());
    		 
    		 preparedStatement.executeUpdate(); 
    		 
    		 resultSet = preparedStatement.getGeneratedKeys();
    		 if (resultSet.next()) {
    			 docId = resultSet.getInt(1);
    			 handlerContext.getLogger().log("docId: " + docId);
    		 }
		}
		
		return docId;
	}
	
	private Integer writeToAuroraScan (PageBuilder pageData, List<Block> mlResult, Connection conn) throws SQLException {
    	
    	Integer scanId = null;
    	
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Statement statement = conn.createStatement();
        
        preparedStatement = conn.prepareStatement("insert into tab_scans (SCAN_S3_KEY, SCAN_S3_URL, S3_OBJ_ARN, S3_OBJ_ETAG, SCAN_FILE_PATH, JSON_ML_FILE_PATH, SCAN_S3_BUCKET) values (?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
 		 preparedStatement.setString(1, pageData.getS3Key());
 		 if (pageData.getScanS3URL() == null)  preparedStatement.setNull(2, Types.VARCHAR);
 		 else preparedStatement.setString(2, pageData.getScanS3URL());
 		 if (pageData.getS3ObjArn() == null)  preparedStatement.setNull(3, Types.VARCHAR);
 		 else preparedStatement.setString(3, pageData.getS3ObjArn());
 		 if (pageData.getS3ObjEtag() == null)  preparedStatement.setNull(4, Types.VARCHAR);
 		 else preparedStatement.setString(4, pageData.getS3ObjEtag());
 		 preparedStatement.setString(5, pageData.getScanFilePath());
 		 preparedStatement.setString(6, pageData.getJsonMlFilePath());
 		 preparedStatement.setString(7, pageData.getS3Bucket());
 		 preparedStatement.executeUpdate();//no concurency due to business
 		     		 
 		 resultSet = preparedStatement.getGeneratedKeys();
 		 if (resultSet.next()) {
 			 scanId = resultSet.getInt(1);
 			 handlerContext.getLogger().log("scanId: " + scanId);
 		 }
		
		return scanId;
    }
	
	private Integer writeToAuroraPage (PageBuilder pageData, List<Block> mlResult, Connection conn) throws SQLException {
    	
    	Integer pageId = null;
    	
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Statement statement = conn.createStatement();

		 preparedStatement = conn.prepareStatement("insert into tab_doc_pages (DOC_ID, PAGE_TYPE_ID, PAGE_NUMBER, PAGE_SECTION, SCAN_ID) "
 		 		+ "values (?, (select TYPE_ID from tab_page_types where TYPE_CODE = ?), ?, ?, (select SCAN_ID from tab_scans where SCAN_S3_KEY = ?))", Statement.RETURN_GENERATED_KEYS);
 		 preparedStatement.setInt(1, pageData.getDocumentId());
 		 preparedStatement.setString(2, pageData.getPageType());
 		 preparedStatement.setInt(3, pageData.getPageNumber());
 		 if (pageData.getPageSection() == null) preparedStatement.setNull(4, Types.INTEGER);
 		 	else preparedStatement.setInt(4, pageData.getPageSection());
 		 preparedStatement.setString(5, pageData.getS3Key());
 		 preparedStatement.executeUpdate();//no concurency due to business
 		 
 		 resultSet = preparedStatement.getGeneratedKeys();
 		 if (resultSet.next()) {
 			 pageId = resultSet.getInt(1);
 			 handlerContext.getLogger().log("pageId: " + pageId);
 		 }
 		 
		return pageId;
    }
	
	private Integer writeToAuroraPageLines (PageBuilder pageData, List<Block> mlResult, Connection conn) throws SQLException {
    	
    	int linesCount = 0;
    	
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Statement statement = conn.createStatement();

		 preparedStatement = conn.prepareStatement("insert into tab_doc_pag_lines (PAGE_ID, LINE_ML_TEXT, LINE_ML_ID, LINE_ML_CONFIDENCE, LINE_ML_INDEX) values (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
		 preparedStatement.setInt(1, pageData.getPageId());
		     		 
		 for (Block block : mlResult) {
       	              	
        	if (("LINE".equals(block.getBlockType()))) {
        		
        		//handlerContext.getLogger().log("id: " + block.getId() + " text: " + block.getText() + " confidence: " + block.getConfidence() + " sorter: " + mlResult.indexOf(block));
        		             		
        		preparedStatement.setString(2, block.getText());
        		preparedStatement.setString(3, block.getId());
        		if (block.getConfidence() == null) preparedStatement.setNull(3, Types.FLOAT);
        		else preparedStatement.setFloat(4, block.getConfidence());
        		preparedStatement.setInt(5, mlResult.indexOf(block) + 1);
        		preparedStatement.executeUpdate();//no concurency due to business
        		linesCount++;
            }
        }
 		 
		return Integer.valueOf(linesCount);
    }
    
    /**
     * programmatically build the SQL query to find the company into the database
     * @param company the string that contains one company names; taken from file name and separated by comma
     * @return the SQL query
     */
    public String getQueryToCheckEqualCompany (String company) {
    	StringBuilder query = new StringBuilder("select COMPANY_ID from tab_companies where COMPANY_NAME_CODES in ");
    	    	
    	if (company.contains(",")) {
    		query.append("(");
    		    			
			List<String> namePerms = MathLambda.permute(company);
			
			for (String namePerm : namePerms) {
				query.append(namePerm).append(", ");
			}
    		
    		query.deleteCharAt(query.length()-2);
    		query.deleteCharAt(query.length()-1);
    		query.append(")");
    		
    	} else {
    		query.append("('" + company + "')");
    	}
    	
    	return query.toString();
    }
    
    /**
     * programmatically build the SQL query to find a similar company into the database
     * @param company company the string that contains one company names; taken from file name and separated by comma
     * @return the SQL query
     */
    public String getQueryToCheckLikeCompany (String company) {
    	StringBuilder query = new StringBuilder("select COMPANY_ID from tab_companies where ");
    	
    	String[] compNames = company.split(",");
    	
    	query.append("COMPANY_NAME_CODES like '%" + compNames[0] + "%'");
    	
    	if (compNames.length > 1) {
    		
    		for (int i = 1; i < compNames.length; i++ ) {
    			
    			query.append(" and COMPANY_NAME_CODES like '%" + compNames[i] + "%'");
    		}
    	}
    	
    	return query.toString();
    }
       

	public Context getHandlerContext() {
		return handlerContext;
	}

	public void setHandlerContext(Context handlerContext) {
		this.handlerContext = handlerContext;
	}
    
    
}
