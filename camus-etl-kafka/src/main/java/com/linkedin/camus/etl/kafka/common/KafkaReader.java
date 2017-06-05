package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import static kafka.api.OffsetRequest.CurrentVersion;
import static kafka.api.OffsetRequest.DefaultClientId;

/**
 * Poorly named class that handles kafka pull events within each
 * KafkaRecordReader.
 *
 * @author Richard Park
 */
public class KafkaReader {
	// index of context
	private static Logger log =  Logger.getLogger(KafkaReader.class);
	private final int[] DELAYS_MS = {2, 512, 1024, 2048, 4096, 8192, 16384};

	private EtlRequest kafkaRequest = null;
	private SimpleConsumer simpleConsumer = null;

	private long beginOffset;
	private long currentOffset;
	private long lastOffset;
	private long currentCount;

	private TaskAttemptContext context;

	private Iterator<MessageAndOffset> messageIter = null;

	private long totalFetchTime = 0;
	private long lastFetchTime = 0;

	private int fetchBufferSize;

	/**
	 * Construct using the json representation of the kafka request
	 */
	public KafkaReader(TaskAttemptContext context, EtlRequest request,
			int clientTimeout, int fetchBufferSize) throws Exception {
		this.fetchBufferSize = fetchBufferSize;
		this.context = context;

		log.info("bufferSize=" + fetchBufferSize);
		log.info("timeout=" + clientTimeout);

		// Create the kafka request from the json

		kafkaRequest = request;

		beginOffset = request.getOffset();
		currentOffset = request.getOffset();
		lastOffset = request.getLastOffset();
		currentCount = 0;
		totalFetchTime = 0;

		// read data from queue

		URI uri = kafkaRequest.getURI();
		simpleConsumer = new SimpleConsumer(uri.getHost(), uri.getPort(),
				CamusJob.getKafkaTimeoutValue(context),
				CamusJob.getKafkaBufferSize(context),
				CamusJob.getKafkaClientName(context));
		log.info("Connected to leader " + uri
				+ " beginning reading at offset " + beginOffset
				+ " latest offset=" + lastOffset);
        registerMetrics(request, simpleConsumer);
		fetch();
	}

    private void registerMetrics(final EtlRequest request, final SimpleConsumer simpleConsumer) {
        MetricName currentOffsetMetricName = new MetricName(request.getTopic(), "currentOffset", "partition-" + request.getPartition());
        MetricName lagMetricName = new MetricName(request.getTopic(), "lag", "partition-" + request.getPartition());
        Metrics.defaultRegistry().removeMetric(currentOffsetMetricName);
        Metrics.defaultRegistry().newGauge(currentOffsetMetricName, new Gauge<Long>() {
            @Override
            public Long value() {
                return currentOffset;
            }
        });
        Metrics.defaultRegistry().newGauge(lagMetricName, new Gauge<Long>() {
            @Override
            public Long value() {
                try {
                    long last = getLatestOffset(request.getTopic(), request.getPartition(), simpleConsumer);
                    return last - currentOffset;
                } catch (Exception e) {
                    log.warn("Can't get topic latest offset");
                    return null;
                }
            }
        });
    }

    public static long getLatestOffset(String topic, int partition, SimpleConsumer leader) throws Exception {
        long time = kafka.api.OffsetRequest.LatestTime();
        TopicAndPartition tp = new TopicAndPartition(topic, partition);
        PartitionOffsetRequestInfo requestInfo = new PartitionOffsetRequestInfo(time, 1);
        OffsetRequest request = new OffsetRequest(Collections.singletonMap(tp, requestInfo), CurrentVersion(), DefaultClientId());
        OffsetResponse response = leader.getOffsetsBefore(request);

        if (response.hasError()) {
            final short errorCode = response.errorCode(tp.topic(), tp.partition());
            Exception ex = (Exception) ErrorMapping.exceptionFor(errorCode);
            throw ex;
        } else {
            long offset = response.offsets(tp.topic(), tp.partition())[0];
            return offset;
        }
    }

    public boolean hasNext() throws IOException {
		if (messageIter != null && messageIter.hasNext())
			return true;
		else
			return fetch();

	}

	/**
	 * Fetches the next Kafka message and stuffs the results into the key and
	 * value
	 *
	 * @param key
	 * @param payload
	 * @param pKey
	 * @return true if there exists more events
	 * @throws IOException
	 */
	public boolean getNext(EtlKey key, BytesWritable payload ,BytesWritable pKey) throws IOException {
		if (hasNext()) {

			MessageAndOffset msgAndOffset = messageIter.next();
			Message message = msgAndOffset.message();

			ByteBuffer buf = message.payload();
			int origSize = buf.remaining();
			byte[] bytes = new byte[origSize];
			buf.get(bytes, buf.position(), origSize);
			payload.set(bytes, 0, origSize);

			buf = message.key();
			if(buf != null){
				origSize = buf.remaining();
				bytes = new byte[origSize];
				buf.get(bytes, buf.position(), origSize);
				pKey.set(bytes, 0, origSize);
			}

			key.clear();
			key.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(),
					kafkaRequest.getPartition(), currentOffset,
					msgAndOffset.offset() + 1, message.checksum());

			currentOffset = msgAndOffset.offset() + 1; // increase offset
			currentCount++; // increase count

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Creates a fetch request.
	 *
	 * @return false if there's no more fetches
	 * @throws IOException
	 */

	public boolean fetch() throws IOException {
/*		if (currentOffset >= lastOffset) {
			return false;
		}*/
		long tempTime = System.currentTimeMillis();
		TopicAndPartition topicAndPartition = new TopicAndPartition(
				kafkaRequest.getTopic(), kafkaRequest.getPartition());
		log.info("Asking for offset : " + (currentOffset));
		PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(
				currentOffset, fetchBufferSize);

		HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
		fetchInfo.put(topicAndPartition, partitionFetchInfo);

		FetchRequest fetchRequest = new FetchRequest(
				CamusJob.getKafkaFetchRequestCorrelationId(context),
				CamusJob.getKafkaClientName(context),
				CamusJob.getKafkaFetchRequestMaxWait(context),
				CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);

		FetchResponse fetchResponse = null;
        long startTime = System.currentTimeMillis();

        int tryCount = 0;
        while (System.currentTimeMillis() - startTime < CamusJob.getKafkaFetchRequestMaxWait(context)) {
            try {
                fetchResponse = simpleConsumer.fetch(fetchRequest);
                if (fetchResponse.hasError()) {
                    log.warn("Error encountered during a fetch request from Kafka");
                    log.warn("Error Code generated : "
                            + fetchResponse.errorCode(kafkaRequest.getTopic(), kafkaRequest.getPartition())
                    );
                    return false;
                } else {
                    ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(
                            kafkaRequest.getTopic(), kafkaRequest.getPartition());
                    lastFetchTime = (System.currentTimeMillis() - tempTime);
                    log.debug("Time taken to fetch : " + (lastFetchTime / 1000) + " seconds");
                    log.debug("The size of the ByteBufferMessageSet returned is : " + messageBuffer.sizeInBytes());
                    int skipped = 0;
                    totalFetchTime += lastFetchTime;
                    messageIter = messageBuffer.iterator();
                    //boolean flag = false;
                    Iterator<MessageAndOffset> messageIter2 = messageBuffer
                            .iterator();
                    MessageAndOffset message = null;
                    int messageCount = 0;
                    while (messageIter2.hasNext()) {
                    	messageCount++;
                        message = messageIter2.next();
                        if (message.offset() < currentOffset) {
                            //flag = true;
                            skipped++;
                        } else {
                            log.debug("Skipped offsets till : " + message.offset());
                            break;
                        }
                    }
                    log.info("Messages fetched: " + messageCount);
                    log.debug("Number of offsets to be skipped: " + skipped);
                    while(skipped !=0 )
                    {
                        MessageAndOffset skippedMessage = messageIter.next();
                        log.info("Skipping offset : " + skippedMessage.offset());
                        skipped --;
                    }

                    if (messageIter.hasNext()) {
                        return true;
                    }

                    log.warn("No messages received");
                    log.warn("Received bytes: " + messageBuffer.sizeInBytes()
                            + ". Received valid bytes: " + messageBuffer.validBytes());

                    int waitTime = tryCount < DELAYS_MS.length ? DELAYS_MS[tryCount] : DELAYS_MS[DELAYS_MS.length -1];
                    log.warn("Waiting for " + waitTime + " ms. before next try");
                    Thread.sleep(waitTime);
                    tryCount++;
                }
            } catch (Exception e) {
                log.info("Exception generated during fetch", e);
                e.printStackTrace();
                return false;
            }
        }
        System.out.println("No more data left to process. Returning false");
        messageIter = null;
        return false;
	}

	/**
	 * Closes this context
	 *
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (simpleConsumer != null) {
			simpleConsumer.close();
		}
	}

	/**
	 * Returns the total bytes that will be fetched. This is calculated by
	 * taking the diffs of the offsets
	 *
	 * @return
	 */
	public long getTotalBytes() {
		return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
	}

	/**
	 * Returns the total bytes that have been fetched so far
	 *
	 * @return
	 */
	public long getReadBytes() {
		return currentOffset - beginOffset;
	}

	/**
	 * Returns the number of events that have been read r
	 *
	 * @return
	 */
	public long getCount() {
		return currentCount;
	}

	/**
	 * Returns the fetch time of the last fetch in ms
	 *
	 * @return
	 */
	public long getFetchTime() {
		return lastFetchTime;
	}

	/**
	 * Returns the totalFetchTime in ms
	 *
	 * @return
	 */
	public long getTotalFetchTime() {
		return totalFetchTime;
	}
}
