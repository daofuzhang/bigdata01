package com.want.bigdata.service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.sap.conn.jco.JCoException;
import com.sap.tc.logging.Location;

@Component
public class JobRunner implements CommandLineRunner {

	private Location logger =Location.getLocation(JobRunner.class);
	
	@Autowired
	private DbService dbService;
	@Autowired
	private JcoService jcoService;
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	@Override
	public void run(String... args) throws Exception {
		
		logger.infoT("begin run >>>"+sdf.format(new Date()) +" params:"+args);
		if(null!=args && args.length>0) {
			if("115".equals(args[0])) {
				runJCO115();
			}else if("037".equals(args[0])){
				exec("H11");
			}
		}
		logger.infoT("end run >>>"+sdf.format(new Date()) );
	}
	
	public void runJCO115() throws JCoException {
		String rfcName="ZERPRFC_115";
		Map<String, String> queryMapInput =new HashMap<String,String>();
		queryMapInput.put("I_VKORG", "C111");
		Map<String, String> queryMapOutput =null;
		Map<String,Queue<Map<String, String>>> inputTables =null;
		Map<String,Queue<Map<String, String>>> outputTables=new HashMap<String,Queue<Map<String, String>>>();
		outputTables.put("T_DATA", null);
		jcoService.getResult(rfcName, queryMapInput, inputTables, queryMapOutput, outputTables);
		Queue<Map<String, String>> queue = outputTables.get("T_DATA");
		while(queue.size()!=0) {
			Map<String, String> poll = queue.poll();
			for(String name:poll.keySet()) {
				System.out.println(name+":"+poll.get(name));
			}
		}
	}
	public void exec(String werks) throws JCoException {
		Map<String, String> queryMapInput =null;
		Map<String, String> queryMapOutput =new HashMap<String, String>();
		queryMapOutput.put("EV_ZRETURNCODE", null);
		queryMapOutput.put("EV_ZRETURNMSG", null);
		Map<String,Queue<Map<String, String>>> inputTables =new HashMap<String,Queue<Map<String, String>>>();
		Map<String, String> inputParams =new HashMap<String, String>();
		inputParams.put("WERKS", werks);
		Queue<Map<String, String>> q=new LinkedBlockingQueue<Map<String, String>>();
		q.add(inputParams);
		inputTables.put("I_WERKS", q);
		
		Map<String,Queue<Map<String, String>>> outputTables=new HashMap<String,Queue<Map<String, String>>>();
		outputTables.put("T_ZHRS054", null);
		jcoService.getResult("ZRFCHR037", queryMapInput, inputTables, queryMapOutput, outputTables);
		Queue<Map<String, String>> queue = outputTables.get("ZHRS054");
		while(queue.size()!=0) {
			Map<String, String> zhrs054Map =queue.poll();
			for(String name:zhrs054Map.keySet()) {
				logger.errorT(name+":"+zhrs054Map.get(name));
			}
		}	
	}

}
