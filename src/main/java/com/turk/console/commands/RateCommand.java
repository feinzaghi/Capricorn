package com.turk.console.commands;

import java.io.InputStreamReader;   
import java.io.LineNumberReader;

import com.turk.console.common.console.io.CommandIO;


public class RateCommand extends BasicCommand{

	private static final int CPUTIME = 30; 
	private static final int PERCENT = 100;
	private static final int FAULTLENGTH = 10; 
	public static Thread thread;

	@Override
	public boolean doCommand(String[] args,final CommandIO io) throws Exception {
		
		RateCommand.thread = new Thread(new Runnable()
 		{
			public void run()
 			{
				while(true){
					io.println("\n");
					io.println(getMemery());
					io.println(getCpuRatioForWindows());
					
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
 			}
 		});
		RateCommand.thread.start();
		return true;
		
	}
	private String getMemery(){
		
		/*
		OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory   
	    .getOperatingSystemMXBean();   

		// 总的物理内存   
		long totalMemorySize = osmxb.getTotalPhysicalMemorySize()/1048576L;   
		// 剩余的物理内存   
		long freePhysicalMemorySize = osmxb.getFreePhysicalMemorySize()/1048576L;   
		// 已使用的物理内存   
		long usedMemory = (osmxb.getTotalPhysicalMemorySize() - osmxb.getFreePhysicalMemorySize())/1048576L; 
		double memoryRate=usedMemory*100/totalMemorySize;
		return "总物理内存："+totalMemorySize+" M\r\n   剩余物理内存："+freePhysicalMemorySize+" M\r\n   已使用的物理内存："+usedMemory+" M\r\n   内存使用率："+memoryRate+"%";
		*/
		return "";
	}
	/**  
     * 获得CPU使用率.  
     * @return 返回cpu使用率  
     * @author   
     */   
    private String getCpuRatioForWindows() {   
        try {   
            String procCmd = System.getenv("windir")   
                    + "\\system32\\wbem\\wmic.exe process get Caption,CommandLine,"   
                    + "KernelModeTime,ReadOperationCount,ThreadCount,UserModeTime,WriteOperationCount";   
            // 取进程信息   
            long[] c0 = readCpu(Runtime.getRuntime().exec(procCmd));   
            Thread.sleep(CPUTIME);   
            long[] c1 = readCpu(Runtime.getRuntime().exec(procCmd));   
            if (c0 != null && c1 != null) {   
                long idletime = c1[0] - c0[0];   
                long busytime = c1[1] - c0[1];   
                return "CPU使用率:"+Double.valueOf(PERCENT * (busytime) / (busytime + idletime)).doubleValue()+"%";   
            } else {   
                return "CPU使用率:"+0.0+"%";   
            }   
        } catch (Exception ex) {   
            ex.printStackTrace();   
            return "CPU使用率:"+0.0+"%";   
        }   
    }  
    
    /**
     * 读取CPU信息.  
     * @param proc  
     * @return  
     * @author   
     */   
    private long[] readCpu(final Process proc) {   
        long[] retn = new long[2];   
        try {   
            proc.getOutputStream().close();   
            InputStreamReader ir = new InputStreamReader(proc.getInputStream());   
            LineNumberReader input = new LineNumberReader(ir);   
            String line = input.readLine();   
            if (line == null || line.length() < FAULTLENGTH) {   
                return null;   
            }   
            int capidx = line.indexOf("Caption");   
            int cmdidx = line.indexOf("CommandLine");   
            int rocidx = line.indexOf("ReadOperationCount");   
            int umtidx = line.indexOf("UserModeTime");   
            int kmtidx = line.indexOf("KernelModeTime");   
            int wocidx = line.indexOf("WriteOperationCount");   
            long idletime = 0;   
            long kneltime = 0;   
            long usertime = 0;   
            while ((line = input.readLine()) != null) {   
                if (line.length() < wocidx) {   
                    continue;   
                }   
                // 字段出现顺序：Caption,CommandLine,KernelModeTime,ReadOperationCount,   
                // ThreadCount,UserModeTime,WriteOperation   
                String caption = Bytes.substring(line, capidx, cmdidx - 1)   
                        .trim();   
                String cmd = Bytes.substring(line, cmdidx, kmtidx - 1).trim();   
                if (cmd.indexOf("wmic.exe") >= 0) {   
                    continue;   
                }   
                // log.info("line="+line);   
                if (caption.equals("System Idle Process")   
                        || caption.equals("System")) {   
                    idletime += Long.valueOf(   
                            Bytes.substring(line, kmtidx, rocidx - 1).trim())   
                            .longValue();   
                    idletime += Long.valueOf(   
                            Bytes.substring(line, umtidx, wocidx - 1).trim())   
                            .longValue();   
                    continue;   
                }   
  
                kneltime += Long.valueOf(   
                        Bytes.substring(line, kmtidx, rocidx - 1).trim())   
                        .longValue();   
                usertime += Long.valueOf(   
                        Bytes.substring(line, umtidx, wocidx - 1).trim())   
                        .longValue();   
            }   
            retn[0] = idletime;   
            retn[1] = kneltime + usertime;   
            return retn;   
        } catch (Exception ex) {   
            ex.printStackTrace();   
        } finally {   
            try {   
                proc.getInputStream().close();   
            } catch (Exception e) {   
                e.printStackTrace();   
            }   
        }   
        return null;   
    }  
}

 class Bytes {   
    public static String substring(String src, int start_idx, int end_idx){   
        byte[] b = src.getBytes();   
        String tgt = "";   
        for(int i=start_idx; i<=end_idx; i++){   
            tgt +=(char)b[i];   
        }   
        return tgt;   
    }   
} 
