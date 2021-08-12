import java.lang.management.ManagementFactory;

public class MeasureSourceConsumption {

	private Integer cpuCount;
	private Long startCPUTime;
	private Long startTime;
	private Long totalProcessTime;
	private Long totalAvailCPUTime;
	private Long totalUsedCPUTime;
	private Long totalMemory;
	private Long startFreeMemory;
	private Long endFreeMemory;
	private String consumptions;

	public MeasureSourceConsumption() {
		this.consumptions = "Consumptions: " + "\n";
	}

	public void startCalculateTimeAndCpuConsumption() {
		int mb = 1024*1024;
		cpuCount = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
		startCPUTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
		startTime = System.nanoTime();
		startFreeMemory = Runtime.getRuntime().freeMemory() / mb;
	}

	public void endCalculateTimeAndCpuConsumption() {
		int mb = 1024*1024;
		long endTime = System.nanoTime();
		totalProcessTime = (endTime - startTime) / 1000000;

		totalAvailCPUTime = cpuCount * (endTime-startTime);
		totalUsedCPUTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime()-startCPUTime;
		endFreeMemory = Runtime.getRuntime().freeMemory() / mb;
		totalMemory = Runtime.getRuntime().totalMemory() / mb;
	}

	public void printConsumptions(Integer solutionSize) {
		consumptions += "Total Process Time: " + totalProcessTime + " ms" + "\n";
		consumptions += "Total CPU Consumption: " + totalUsedCPUTime + "\n";
		consumptions += "Total Memory Consumption: " + totalMemory + "\n";
		consumptions += "Memory Used: " + (startFreeMemory - endFreeMemory) + "\n";
		consumptions += "Number of Result: " + (solutionSize) + "\n";

		System.out.println(consumptions);
	}

	public String measureMemoryConsumption(Runtime runtime) {
		long total, free, used;
		int mb = 1024*1024;
		String response = "";

		total = runtime.totalMemory();
		free = runtime.freeMemory();
		used = total - free;
		response += "Total Memory: " + total / mb + "MB\n";
		response += "Memory Used: " + used / mb + "MB\n";
		response += "Memory Free: " + free / mb + "MB\n";
		response += "Percent Used: " + ((double)used/(double)total)*100 + "%\n";
		return response;
	}

	public int measureCPUConsumption(long cpuStartTime, long elapsedStartTime, int cpuCount) {
		long end = System.nanoTime();
		long totalAvailCPUTime = cpuCount * (end-elapsedStartTime);
		long totalUsedCPUTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime()-cpuStartTime;
		float per = ((float)totalUsedCPUTime*100)/(float)totalAvailCPUTime;
		return (int)per;
	}

}
