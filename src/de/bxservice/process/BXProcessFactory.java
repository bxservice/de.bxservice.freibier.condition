package de.bxservice.process;

import org.adempiere.base.IProcessFactory;
import org.compiere.process.ProcessCall;

public class BXProcessFactory implements IProcessFactory{
	@Override
	public ProcessCall newProcessInstance(String className) {
		ProcessCall process = null;
		if ("de.bxservice.process.BXFixPriceListProcess".equals(className)) {
			try {
				process =  BXFixPriceListProcess.class.newInstance();
			} catch (Exception e) {}
		}
		if ("de.bxservice.process.BXFillVendorBreak".equals(className)) {
			try {
				process =  BXFillVendorBreak.class.newInstance();
			} catch (Exception e) {}
		}
		return process;
	}
}