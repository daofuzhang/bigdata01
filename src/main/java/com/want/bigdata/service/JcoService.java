package com.want.bigdata.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoMetaData;
import com.sap.conn.jco.JCoParameterList;
import com.sap.conn.jco.JCoTable;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;


@Service
public class JcoService {

	@Autowired
	private JcoConfig jcoConfig;
	
	public static final String ABAP_AS_POOLED = "ABAP_AS_POOL";
	
	private JCoFunction getJCoFunction(String functionName) throws JCoException {

		Environment.registerDestinationDataProvider(new DestinationDataProvider() {
			@Override
			public Properties getDestinationProperties(String arg0) {
				Properties connectProperties = new Properties();
				connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, jcoConfig.getHost());
				connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, jcoConfig.getSystem());
				connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, jcoConfig.getClient());
				connectProperties.setProperty(DestinationDataProvider.JCO_USER, jcoConfig.getUsername());
				connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, jcoConfig.getPassword());
				connectProperties.setProperty(DestinationDataProvider.JCO_LANG, jcoConfig.getLang());
				connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, "3");
		        connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT, "5");
				return connectProperties;
			}
			@Override
			public void setDestinationDataEventListener(DestinationDataEventListener el) {
				el.updated(ABAP_AS_POOLED);
			}
			@Override
			public boolean supportsEvents() {
				return true;
			}
		});
		JCoDestination destination = JCoDestinationManager.getDestination(ABAP_AS_POOLED);
		JCoFunction function = destination.getRepository().getFunctionTemplate(functionName).getFunction();
		return function;
	}
	/**
	 * 
	 * @param rfcName  rfc名称
	 * @param queryMapInput 入参 单数字 : WERKS:C111
	 * @param inputTables   入参表: OT_WERKS ： <WERKS:C111> <MATNR:112345677>
	 * @param queryMapOutput 出参 单数字 : WERKS:C111
	 * @param outputTables 出参表: OT_WERKS ： <WERKS:C111> <MATNR:112345677>
	 * @throws JCoException
	 */
	public void getResult(String rfcName,Map<String, String> queryMapInput,Map<String,Queue<Map<String, String>>> inputTables,Map<String, String> queryMapOutput,Map<String,Queue<Map<String, String>>> outputTables) throws JCoException{
		JCoFunction jCoFunction =getJCoFunction(rfcName);
		//入参
		JCoParameterList input = jCoFunction.getImportParameterList();
		if (queryMapInput != null) {
			Set<String> parmakeys = queryMapInput.keySet();
			for (String parmakey : parmakeys) {
				input.setValue(parmakey, queryMapInput.get(parmakey));
			}
		}
		//表结构
		 //入参
		if(null!=inputTables) {
			for(String tblName:inputTables.keySet()) {
				JCoTable tables = jCoFunction.getTableParameterList().getTable(tblName);
				Queue<Map<String, String>> queue = inputTables.get(tblName);
				while(queue.size()!=0) {
					Map<String, String> paraminut =queue.poll();
					for(String field:paraminut.keySet()) {
						tables.appendRow();
						tables.setValue(field, paraminut.get(field));
					}
				}
			}
		}
		JCoDestination dest=JCoDestinationManager.getDestination(ABAP_AS_POOLED);
		jCoFunction.execute(dest);
		//出参
		JCoParameterList params = jCoFunction.getExportParameterList();
		if (params != null) {
			for (JCoField jCoField : params) {
				queryMapOutput.put(jCoField.getName(), jCoField.getString());
			}
		}
		//出参
		if(null!=outputTables) {
			for(String tblName:outputTables.keySet()) {
				JCoTable tables = jCoFunction.getTableParameterList().getTable(tblName);
				Queue<Map<String, String>> resultQueue = new LinkedBlockingQueue<Map<String, String>>();
				JCoMetaData imd = tables.getMetaData();
				String fields[] = new String[imd.getFieldCount()];
				for (int j = 0; j < imd.getFieldCount(); j++) {
					fields[j] = imd.getName(j);
				}
				for (int i = 0; i < tables.getNumRows(); i++) {
					tables.setRow(i);
					HashMap<String, String> datahm = new HashMap<String, String>(imd.getFieldCount());
					for (int z = 0; z < fields.length; z++) {
						datahm.put(fields[z], tables.getString(fields[z]));
					}
					resultQueue.add(datahm);
				}
				outputTables.put(tblName, resultQueue);
			}
		}
	}
	
}
