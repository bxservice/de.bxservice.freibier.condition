package de.bxservice.process;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;

import org.compiere.model.X_M_ProductPriceVendorBreak;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;

public class BXFillVendorBreak extends SvrProcess{

	/**	Order Date From		*/
	private Timestamp	p_DateFrom;

	@Override
	protected void prepare() {

		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null && para[i].getParameter_To() == null)
				;
			else if (name.equals("DateFrom"))
			{
				p_DateFrom = (Timestamp)para[i].getParameter();
			}
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}

	}
	
	@Override
	protected String doIt() throws Exception {

		if (log.isLoggable(Level.INFO)) 
			log.info("DateFrom=" + p_DateFrom);
		
		StringBuilder sql = new StringBuilder(); 
		sql.append("SELECT DISTINCT ON (condition.M_Product_ID, condition.Customer_ID, version.M_PriceList_Version_ID) condition.M_Product_ID, "
				        + "condition.Customer_ID, "
				        + "version.M_PriceList_Version_ID, "
				        + "condition.amount, "
				        + "condition.qtybreak "
				+ " FROM BAY_Condition condition, M_PriceList_Version version ");
		StringBuilder whereClause = new StringBuilder();
		whereClause.append("WHERE condition.BAY_ConditionStage_ID = (SELECT BAY_ConditionStage_ID FROM BAY_ConditionStage WHERE value like 'SalesVendorBreak') "
				+ "AND condition.isActive = 'Y' "
				+ "AND condition.Customer_ID IS NOT NULL "
				+ "AND condition.DateFrom >= ? "
				+ "AND version.M_PriceList_ID = condition.M_PriceList_ID ");

		sql.append(whereClause);

		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int cnt = 0;
		int cntUpdated = 0;
		
		try {
			pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
			pstmt.setTimestamp(1, p_DateFrom);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Integer productId = rs.getInt(1);
				Integer customerId = rs.getInt(2);
				Integer priceListVersionId = rs.getInt(3);				
				BigDecimal amount = rs.getBigDecimal(4);
				BigDecimal qtyBreak = rs.getBigDecimal(5);
				
				if( productId != null && customerId != null && priceListVersionId != null && amount != null && qtyBreak != null){
					
					int breakID = getExistingPriceBreakId(productId, customerId, priceListVersionId);
					X_M_ProductPriceVendorBreak vendorBreak = new X_M_ProductPriceVendorBreak(Env.getCtx(), breakID, get_TrxName());
					
					if(breakID == 0){
						vendorBreak.setM_Product_ID(productId);
						vendorBreak.setM_PriceList_Version_ID(priceListVersionId);
						vendorBreak.setC_BPartner_ID(customerId);
						vendorBreak.setBreakValue(qtyBreak);
						vendorBreak.setPriceList(amount);
						vendorBreak.setPriceLimit(amount);
						vendorBreak.setPriceStd(amount);
						vendorBreak.saveEx();
						cnt++;

					}else {
						vendorBreak.setBreakValue(qtyBreak);
						vendorBreak.setPriceList(amount);
						vendorBreak.setPriceLimit(amount);
						vendorBreak.setPriceStd(amount);
						vendorBreak.saveEx();
						cntUpdated++;
					}

				}
			}

		} catch (SQLException e) {
			log.log(Level.SEVERE, sql.toString(), e);
			DB.rollback(true, get_TrxName());
			throw e;
		} finally {
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}

		return "@M_ProductPriceVendorBreak@ @Inserted@= "+cnt+" @Updated@="+cntUpdated;
	}
	
	public int getExistingPriceBreakId(Integer productId, Integer customerId, Integer priceListVersionId) throws SQLException {
		
		int breakId = 0;
		StringBuilder sql = new StringBuilder(); 
		sql.append("SELECT M_ProductPriceVendorBreak_ID "
				+ " FROM M_ProductPriceVendorBreak ");
		StringBuilder whereClause = new StringBuilder();
		whereClause.append("WHERE M_Product_ID = ? AND C_BPartner_ID = ? AND M_PriceList_Version_ID = ? ");

		sql.append(whereClause);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
			pstmt.setInt(1, productId);
			pstmt.setInt(2, customerId);
			pstmt.setInt(3, priceListVersionId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				breakId = rs.getInt(1);
			}
		} catch (SQLException e) {
			log.log(Level.SEVERE, sql.toString(), e);
			throw e;
		} finally {
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}
		
		return breakId;
	}

}