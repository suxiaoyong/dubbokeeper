package com.dubboclub.dk.monitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.dubboclub.dk.storage.model.Statistics;
import com.dubboclub.dk.storage.model.Statistics.ApplicationType;

/**
 * 记录statistics到日志，便于监控
 * 
 * @author suxiaoyong
 *
 */
public class LogStatistics {
	private static Logger logger = LoggerFactory.getLogger(LogStatistics.class);

	InfluxDB influxDB;
	String dbName;
	Executor pool = Executors.newSingleThreadExecutor();
	Set<String> logToInfluxDBSet = new HashSet<>();

	public void init() {

		dbName = ConfigUtils.getProperty("influxdb.database.name", "dubbo");
		for (String method : ConfigUtils.getProperty("log.to.influxdb.method",
				"dubbo").split(",")) {
			logToInfluxDBSet.add(method);
		}

		influxDB = InfluxDBFactory.connect(ConfigUtils.getProperty(
				"influxdb.host", "http://127.0.0.1:8086"), ConfigUtils
				.getProperty("influxdb.username", "root"), ConfigUtils
				.getProperty("influxdb.password", "root"));
		List<String> dbList = influxDB.describeDatabases();

		if (!dbList.contains(dbName)) {
			influxDB.createDatabase(dbName);
		}
		influxDB.enableBatch(2000, 1000, TimeUnit.MILLISECONDS);
	}

	public void log(Statistics stat) {
		pool.execute(() -> write(stat));
	}

	public void write(Statistics stat) {
		String method = stat.getServiceInterface() + "." + stat.getMethod();
		String simpleMethod = stat.getServiceInterface();
		if (simpleMethod.indexOf(".") > 0) {
			simpleMethod = simpleMethod
					.substring(simpleMethod.lastIndexOf(".") + 1)
					+ "." + stat.getMethod();
		}

		if (stat.getType().equals(ApplicationType.CONSUMER)
				|| !logToInfluxDBSet.contains(method) || stat.getElapsed() == 0)
			return;
		Point point = Point
				.measurement("dubbo_method_statistics")
				.time(stat.getTimestamp(), TimeUnit.MILLISECONDS)
				.tag("method", simpleMethod)
				.tag("provier_app", stat.getApplication())
				.tag("provider_server", stat.getRemoteAddress())
				.addField("success_count", stat.getSuccessCount())
				.addField("failure_count", stat.getFailureCount())
				.addField("tps", stat.getTps())
				.addField("concurrent_count", stat.getConcurrent())
				.addField("avg_elapsed", stat.getElapsed())
				.addField("total_elapsed",
						stat.getElapsed() * stat.getSuccessCount())
				.build();
		influxDB.write(dbName, "default", point);
	}

}
