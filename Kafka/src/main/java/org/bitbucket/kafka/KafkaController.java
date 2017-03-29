package org.bitbucket.kafka;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


public class KafkaController {
	private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
	//test cases

	
	private String topicName="kafkaTopic"; 
	//new test cases111
  
	public Runnable createProducer() {
		return this.new Producer();
	}

	public Runnable createConsumer() {
		return this.new Consumer();
	}

	public class Consumer implements Runnable {

		@Override
		public void run() {
		    //access kafka brokers
			 Properties props = new Properties();
		     props.put("bootstrap.servers", "localhost:9092");
		     props.put("group.id", "test");
		     props.put("enable.auto.commit", "true");
		     props.put("auto.commit.interval.ms", "1000");
		     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		     consumer.subscribe(Arrays.asList(topicName));
		     while (true) {
		         ConsumerRecords<String, String> records = consumer.poll(100);
		         for (ConsumerRecord<String, String> record : records)
		        	 
		        	System.out.printf("The offset of the record we just recieved is: offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		     }
		     
		}

	}

	public class Producer implements Runnable {

		@Override
		public void run() {
			/*
			 * NB: This code assumes you have kafka available at the url
			 * address. This one is available through the services in this
			 * project - ie "docker-compose up zookeeper kafka" ** You'll have
			 * to put that into /etc/hosts if running this code from CLI.
			 * and added some new code
			 */
			Properties kprops = new Properties();

			kprops.put("bootstrap.servers",  "localhost:9092");
			kprops.put("acks", "all");
			kprops.put("retries","3");
			kprops.put("batch.size",  "10000");
			kprops.put("linger.ms", "1");
			kprops.put("buffer.memory","10000000");
			kprops.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			kprops.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			// TODO: use ScheduledExecutorService to scheduled fixed runnable
			// https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html
			// TODO: add shutdown hook, to be polite, regarding producer.close
			Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("kafka.producer").build())
					.execute(new Runnable() {
						@Override
						public void run() {
							KafkaProducer<String, String> producer = new KafkaProducer<>(kprops);
							try {
								while (true) {
									logger.trace("==> sending record");
									 
									String now = Instant.now().toString();
									for(int i=0;i<10;i++)
									{
									producer.send(new ProducerRecord<String, String>(topicName,now,new Integer(i).toString()),new Callback(){

						                 public void onCompletion(RecordMetadata metadata, Exception e) {
						                     if(e != null) {
						                        e.printStackTrace();
						                     } else {
						                        logger.info("The offset of the record we just sent is: " + metadata.offset());
						                     }}
									});
									}
									logger.debug("Sent record:  now = {}, start = {}","Sender Message");
									Thread.sleep(5000);
									try {
										Thread.sleep(60000);
									} catch (InterruptedException ex) {
										ex.printStackTrace();
									}

								}
							} catch (InterruptedException e) {
								logger.info("Kafka Producer Interrupted");
							} finally {

								producer.close();
							}
						}

						
					});
		}

	}
   //line2
	public static void main(String[] args) throws Exception {
		KafkaController hb = new KafkaController();
		 hb.createProducer().run();
		 hb.createConsumer().run();

	}

}
