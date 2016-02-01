/*
 * RED5 Open Source Flash Server - https://github.com/Red5/
 * 
 * Copyright 2006-2015 by respective authors (see below). All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.red5.server.scheduling;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.quartz.DateBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerListener;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.listeners.SchedulerListenerSupport;
import org.red5.logging.Red5LoggerFactory;
import org.red5.server.api.scheduling.IScheduledJob;
import org.red5.server.api.scheduling.ISchedulingService;
import org.red5.server.jmx.mxbeans.QuartzSchedulingServiceMXBean;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean;

/**
 * Scheduling service that uses Quartz as backend.
 *
 * @author The Red5 Project
 * @author Joachim Bauch (jojo@struktur.de)
 * @author Paul Gregoire (mondain@gmail.com)
 */
@ManagedResource(objectName = "org.red5.server:name=schedulingService,type=QuartzSchedulingService")
public class QuartzSchedulingService implements ISchedulingService, QuartzSchedulingServiceMXBean, InitializingBean, DisposableBean {

	private final static Logger log = Red5LoggerFactory.getLogger(QuartzSchedulingService.class);
	
	/**
	 * Quartz configuration properties file
	 */
	protected String configFile;	

	/**
	 * Creates schedulers.
	 */
	protected SchedulerFactory factory;

	/**
	 * Creates job detail.
	 */
	protected JobDetailFactoryBean jobDetailfactory;

	/**
	 * Creates triggers.
	 */
	protected SimpleTriggerFactoryBean triggerfactory;
	
	/**
	 * Service scheduler
	 */
	protected Scheduler scheduler;

	/**
	 * Service scheduler listener
	 */
	private SchedulerListener triggerFinalizedListener;

	/**
	 * Instance id
	 */
	protected String instanceId;
	
	/**
	 * Default thread count
	 */
	protected String threadCount = "10";
	
	/**
	 * Storage for job and trigger keys
	 */
	private final ConcurrentMap<String, ScheduledJobKey> keyMap = new ConcurrentHashMap<String, ScheduledJobKey>();

	/** Constructs a new QuartzSchedulingService. */
	public void afterPropertiesSet() throws Exception {
		log.debug("Initializing...");
		try {
			//create the standard factory if we dont have one
			if (factory == null) {
				//set properties
				if (configFile != null) {
					factory = new StdSchedulerFactory(configFile);
				} else {
					Properties props = new Properties();
					props.put("org.quartz.scheduler.instanceName", "Red5_Scheduler");
					props.put("org.quartz.scheduler.instanceId", "AUTO");
					props.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
					props.put("org.quartz.threadPool.threadCount", threadCount);
					props.put("org.quartz.threadPool.threadPriority", "5");
					props.put("org.quartz.jobStore.misfireThreshold", "60000");
					props.put("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
					factory = new StdSchedulerFactory(props);
				}
			}
			if (instanceId == null) {
				scheduler = factory.getScheduler();
			} else {
				scheduler = factory.getScheduler(instanceId);
			}
			//start the scheduler
			if (scheduler != null) {
				triggerFinalizedListener = new TriggerFinalizedListener();
				scheduler.getListenerManager().addSchedulerListener(triggerFinalizedListener);
				scheduler.start();
			} else {
				log.error("Scheduler was not started");
			}
		} catch (SchedulerException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	public void setFactory(SchedulerFactory factory) {
		this.factory = factory;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}
	
//	protected void registerJMX() {
//		//register with jmx server
//		MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
//		try {
//			ObjectName oName = null;
//			if (instanceId == null) {
//				oName = new ObjectName("org.red5.server:name=" + this.getClass().getName());
//			} else {
//				oName = new ObjectName("org.red5.server:name=" + this.getClass().getName() + ",instanceId=" + instanceId);
//			}
//	        mbeanServer.registerMBean(this, oName);
//		} catch (Exception e) {
//			log.warn("Error on jmx registration", e);
//		}		
//	}

	/**
	 * @return the threadCount
	 */
	public String getThreadCount() {
		return threadCount;
	}

	/**
	 * @param threadCount the threadCount to set
	 */
	public void setThreadCount(String threadCount) {
		this.threadCount = threadCount;
	}

	/** {@inheritDoc} */
	public String addScheduledJob(int interval, IScheduledJob job) {
		String name = getJobName();
		JobDetail jobDetail = createJobDetail(name, job);
		// create trigger that fires indefinitely every <interval> milliseconds
		Trigger trigger = TriggerBuilder.newTrigger()
			    .withIdentity(getTriggerName(name))
			    .startAt(DateBuilder.futureDate(1, IntervalUnit.MILLISECOND))
			    .forJob(jobDetail)
			    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
			    		.withIntervalInMilliseconds(interval)
			    		.repeatForever())
			    .build();
		scheduleJob(name, jobDetail, trigger);
		return name;
	}

	/** {@inheritDoc} */
	public String addScheduledOnceJob(Date date, IScheduledJob job) {
		String name = getJobName();	
		JobDetail jobDetail = createJobDetail(name, job);
		// create trigger that fires once
		Trigger trigger = TriggerBuilder.newTrigger()
			    .withIdentity(getTriggerName(name))
			    .startAt(date)
			    .forJob(jobDetail)
			    .build();	
		scheduleJob(name, jobDetail, trigger);
		return name;
	}

	/** {@inheritDoc} */
	public String addScheduledOnceJob(long timeDelta, IScheduledJob job) {
		// Create trigger that fires once in <timeDelta> milliseconds
		return addScheduledOnceJob(new Date(System.currentTimeMillis() + timeDelta), job);
	}

	/** {@inheritDoc} */
	public String addScheduledJobAfterDelay(int interval, IScheduledJob job, int delay) {
		String name = getJobName();
		JobDetail jobDetail = createJobDetail(name, job);
		// Create trigger that fires indefinitely every <interval> milliseconds
		Trigger trigger = TriggerBuilder.newTrigger()
			    .withIdentity(getTriggerName(name))
			    .startAt(DateBuilder.futureDate(delay, IntervalUnit.MILLISECOND))
			    .forJob(jobDetail)
			    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
			    		.withIntervalInMilliseconds(interval)
			    		.repeatForever())
			    .build();
		scheduleJob(name, jobDetail, trigger);
		return name;		
	}

	private JobDetail createJobDetail(String name, IScheduledJob job) {
		// Store reference to applications job and service
		JobDataMap jobData = new JobDataMap();
		jobData.put(QuartzSchedulingServiceJob.SCHEDULING_SERVICE, this);
		jobData.put(QuartzSchedulingServiceJob.SCHEDULED_JOB, job);
		// detail
		//XXX should be here job.getClass()?
		JobDetail jobDetail = JobBuilder.newJob(QuartzSchedulingServiceJob.class)
				.withIdentity(name)
				.usingJobData(jobData)
				.build();
		return jobDetail;
	}

	/**
	 * Getter for job name.
	 *
	 * @return  Job name
	 */
	public String getJobName() {
		return "ScheduledJob_" + UUID.randomUUID();
	}

	private String getTriggerName(String jobName) {
		return "Trigger_" + jobName;
	}

	/** {@inheritDoc} */
	public List<String> getScheduledJobNames() {
		List<String> result = new ArrayList<String>();
		if (scheduler != null) {
			try {
				for (JobKey jobKey : scheduler.getJobKeys(null)) {
					result.add(jobKey.getName());
				}
			} catch (SchedulerException ex) {
				throw new RuntimeException(ex);
			}
		} else {
			log.warn("No scheduler is available");
		}
		return result;
	}

	/** {@inheritDoc} */
	public void pauseScheduledJob(String name) {
		try {
			scheduler.pauseJob(keyMap.get(name).jKey);
		} catch (SchedulerException ex) {
			throw new RuntimeException(ex);
		}
	}

	/** {@inheritDoc} */
	public void resumeScheduledJob(String name) {
		try {
			scheduler.resumeJob(keyMap.get(name).jKey);
		} catch (SchedulerException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void pauseScheduledTrigger(String name) {
		try {
			scheduler.pauseTrigger(keyMap.get(name).tKey);
		} catch (SchedulerException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void resumeScheduledTrigger(String name) {
		try {
			scheduler.resumeTrigger(keyMap.get(name).tKey);
		} catch (SchedulerException ex) {
			throw new RuntimeException(ex);
		}
	}

	/** {@inheritDoc} */
	public void removeScheduledJob(String name) {
		try {
			log.debug("Removing job {}", name);
			ScheduledJobKey key = keyMap.remove(name);
			if (key != null) {
				scheduler.deleteJob(key.jKey);
			} else {
				log.debug("No key found for job: {}", name);
			}
		} catch (SchedulerException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Schedules job and stores it
	 * 
	 * @param name Job name
	 * @param jobDetail Job detail
	 * @param trigger Job trigger
	 */
	private void scheduleJob(String name, JobDetail jobDetail, Trigger trigger) {
		TriggerKey tKey = trigger.getKey();
		JobKey jKey = trigger.getJobKey();

		ScheduledJobKey key = new ScheduledJobKey(tKey, jKey);
		try {
			// schedule
			scheduler.scheduleJob(jobDetail, trigger);
			// store keys by name
			keyMap.put(name, key);
		} catch (SchedulerException ex) {
			throw new RuntimeException(ex);
		}
		if (log.isDebugEnabled()) {
			Class jobClass = null;
			JobDataMap jobDataMap = jobDetail.getJobDataMap();
			if (jobDataMap != null) {
				Object job = jobDataMap.get(QuartzSchedulingServiceJob.SCHEDULED_JOB);
				if (job != null) {
					jobClass = job.getClass();
				}
			}
			log.debug("Scheduled job: job key {}, trigger key {}, job class {}", jKey, tKey, jobClass);
		}
	}

	public void destroy() throws Exception {
		if (scheduler != null) {
			log.debug("Destroying...");
			scheduler.shutdown(false);
		}
		keyMap.clear();
	}

	private class TriggerFinalizedListener extends SchedulerListenerSupport {
		@Override
		public void triggerFinalized(Trigger trigger) {
			TriggerKey key = trigger.getKey();
			String name = trigger.getJobKey().getName();
			log.debug("Trigger was finalized: key {}, job name {}", key, name);
			keyMap.remove(name);
		}
	}

	protected final class ScheduledJobKey {

		TriggerKey tKey;
		
		JobKey jKey;
		
		public ScheduledJobKey(TriggerKey tKey, JobKey jKey) {
			this.tKey = tKey;
			this.jKey = jKey;
		}
		
	}
	
}
