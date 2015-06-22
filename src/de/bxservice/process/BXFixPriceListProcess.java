package de.bxservice.process;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;

import org.compiere.model.MPriceListVersion;
import org.compiere.model.MProductPrice;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;

public class BXFixPriceListProcess extends SvrProcess{

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
		sql.append("SELECT condition.M_Product_ID, condition.M_PriceList_ID, version.M_PriceList_Version_ID, condition.amount "
				+ " FROM BAY_Condition condition, M_PriceList_Version version ");
		StringBuilder whereClause = new StringBuilder();
		whereClause.append("WHERE condition.BAY_ConditionStage_ID = (SELECT BAY_ConditionStage_ID FROM BAY_ConditionStage WHERE value like 'SalesPriceList'"
																	+ " AND AD_CLIENT_ID = ? AND AD_ORG_ID= ?) "
				+ "AND condition.isActive = 'Y' "
				+ "AND condition.M_PriceList_ID IS NOT NULL "
				+ "AND condition.DateFrom >= ? "
				+ "AND version.M_PriceList_ID = condition.M_PriceList_ID");

		sql.append(whereClause);

		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int cnt = 0;
		int cntUpdated = 0;
		
		try {
			pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
			pstmt.setInt(1, Env.getAD_Client_ID(Env.getCtx()) );
			pstmt.setInt(2, Env.getAD_Org_ID(Env.getCtx()) );
			pstmt.setTimestamp(3, p_DateFrom);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Integer productId = rs.getInt(1);
				Integer priceListId = rs.getInt(2);
				Integer priceListVersionId = rs.getInt(3);
				BigDecimal amount = rs.getBigDecimal(4);
				if( productId != null && priceListId != null && priceListVersionId != null && amount != null ){
					MPriceListVersion priceListVersion = new MPriceListVersion(Env.getCtx(), priceListVersionId, get_TrxName());
					MProductPrice[] productPrice = priceListVersion.getProductPrice(" AND M_Product_ID = " + productId);
					
					if(productPrice.length == 0){
						MProductPrice newProductPrice = new MProductPrice(Env.getCtx(), priceListVersionId, productId, get_TrxName());
						newProductPrice.setPriceList(amount);
						newProductPrice.setPriceLimit(amount);
						newProductPrice.setPriceStd(amount);
						newProductPrice.saveEx();
						cnt++;
					}
					else{
						productPrice[0].setPriceLimit(amount);
						productPrice[0].setPriceList(amount);
						productPrice[0].setPriceStd(amount);
						productPrice[0].saveEx();
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

		return "@M_PriceList@ @Inserted@= "+cnt +" @Updated@= "+cntUpdated;
	}

}