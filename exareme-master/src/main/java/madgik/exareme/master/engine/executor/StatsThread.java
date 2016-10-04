package madgik.exareme.master.engine.executor;

import java.io.*;

/**
 * Created by johnchronis on 22/9/2016.
 */
public class StatsThread extends Thread {
    public int count=0;
    public double memmax=0;
    public double cpuavg=0;
    public double initmem =-1;
    public double minmem = Double.MAX_VALUE;
    public void run()  {

        Process process=null;

        while(true) {
            count++;
            try {
                File tempScript = File.createTempFile("statscript", null);

                Writer streamWriter = new OutputStreamWriter(new FileOutputStream(
                        tempScript));
                PrintWriter printWriter = new PrintWriter(streamWriter);

                printWriter.println("#!/bin/bash");

                printWriter.println("np=`nproc`");
                printWriter.println("cpu=`top -bn 1 | awk '{print $9}' | tail -n +8 | awk '{s+=$1;} END {print s}' | cut -f1 -d\".\" | cut -f1 -d\",\"`");
                printWriter.println("avgcpu=$(( cpu / np ))");
                printWriter.println("avgcpu=`echo $avgcpu | cut -f1 -d\".\" | cut -f1 -d\",\"`");

                printWriter.println("totalM=`cat /proc/meminfo | awk '  /MemTotal:/ {print $2}'`");
                printWriter.println("availM=`cat /proc/meminfo | awk '  /MemAvailable:/ {print $2}'`");
                printWriter.println("memperUsed=$(( (100*(totalM-availM)) / totalM ))");
                printWriter.println("echo $avgcpu $memperUsed");

                printWriter.close();

                ProcessBuilder pb = new ProcessBuilder("bash", tempScript.toString());

                process = pb.start();

            } catch (IOException e) {
                e.printStackTrace();
            }

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line = "";
            StringBuilder sb = new StringBuilder();
            try {
                while ((line = reader.readLine()) != null) {
                    sb.append(line + "\n");

                }
            } catch (IOException e) {
                e.printStackTrace();
            }

//                System.out.println(sb.toString());

//                long avgcpuN = Integer.getInteger(sb.toString());
            Integer cpuN;
            try {
                String s1 = sb.toString().split(" ")[0];
                cpuN  = Integer.parseInt(s1);
            }catch(Exception e){
                cpuN = -1;
            }
            Double memN;
            try {
                String s2 = sb.toString().split(" ")[1];
                memN = Double.parseDouble(s2);
            }catch(Exception e){
                memN = -1.0;
            }

            if(initmem ==-1){
                initmem=memN;
            }
            if(minmem>memN){
                minmem=memN;
            }
            if(memN > memmax){
                memmax = memN;
            }
            cpuavg = (((count-1)*cpuavg) + cpuN) / count;

            try {
                process.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
