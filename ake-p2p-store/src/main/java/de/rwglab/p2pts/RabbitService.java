package de.rwglab.p2pts;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import cx.ath.troja.chordless.ChordSet;
import cx.ath.troja.chordless.SingletonEventBus;
import cx.ath.troja.chordless.dhash.DHash;
import cx.ath.troja.chordless.event.DeleteEvent;
import cx.ath.troja.chordless.event.PutEvent;
import cx.ath.troja.chordless.event.ReadEvent;
import cx.ath.troja.chordless.event.UpdateEvent;
import cx.ath.troja.nja.Cerealizer;
import de.rwglab.p2pts.util.InjectLogger;
import org.slf4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class RabbitService extends AbstractService {

	@InjectLogger
	private Logger log;

	private final DHash dHash;

	private final TripleStoreConfig config;

	private Connection connection;

	private Channel channel;

	@Inject
	public RabbitService(final DHash dHash, final TripleStoreConfig config) {
		this.dHash = dHash;
		this.config = config;
	}

	@Subscribe
	public void onRead(ReadEvent readEvent) {

		try {

			BigInteger id = dHash.getIdentifier().getValue();
			String hostName = ((InetSocketAddress) dHash.getServerInfo().getAddress()).getHostName();
			BigInteger key = readEvent.getKey().getValue();
			List<Object> values = newArrayList();

			if (readEvent.getValue() != null) {
				Object unpacked = Cerealizer.unpack(readEvent.getValue().getBytes());
				if (unpacked instanceof ChordSet) {
					for (Object o : (ChordSet) unpacked) {
						values.add(o);
					}
				}
			}

			String json = "{\n"
					+ "\"type\":\"get\",\n"
					+ "\"id\":" + id + ",\n"
					+ "\"hostname\":\"" + hostName + "\",\n"
					+ "\"hash\":" + key + ",\n"
					+ "\"response\":" + Arrays.toString(values.toArray()) + "\n"
					+ "}";

			log.info("readEvent = {}", json);

			publish(json);

		} catch (IOException e) {
			log.error("", e);
		}
	}

	@Subscribe
	public void onPut(PutEvent putEvent) {
		try {

			BigInteger id = dHash.getIdentifier().getValue();
			String hostName = ((InetSocketAddress) dHash.getServerInfo().getAddress()).getHostName();
			BigInteger key = putEvent.getEntry().getIdentifier().getValue();
			Object value = Cerealizer.unpack(putEvent.getEntry().getBytes());

			String json = "{\n"
					+ "\"type\":\"insert\",\n"
					+ "\"id\":" + id + ",\n"
					+ "\"hostname\":\"" + hostName + "\",\n"
					+ "\"hash\":" + key + ",\n"
					+ "\"value\":\"" + value + "\"\n"
					+ "}";

			log.debug("putEvent = {}", json);

			publish(json);

		} catch (IOException e) {
			log.error("", e);
		}
	}

	@Subscribe
	public void onUpdate(UpdateEvent updateEvent) {
		try {

			BigInteger id = dHash.getIdentifier().getValue();
			String hostName = ((InetSocketAddress) dHash.getServerInfo().getAddress()).getHostName();
			BigInteger key = updateEvent.getKey().getValue();
			Object oldValue = updateEvent.getValueRemoved();
			Object value = updateEvent.getValueAdded();

			String json = "{\n"
					+ "\"type\":\"update\",\n"
					+ "\"id\":" + id + ",\n"
					+ "\"hostname\":\"" + hostName + "\",\n"
					+ "\"hash\":" + key + ",\n"
					+ "\"oldValue\":\"" + oldValue + "\",\n"
					+ "\"value\":\"" + value + "\"\n"
					+ "}";

			log.debug("updateEvent = {}", json);

			publish(json);

		} catch (IOException e) {
			log.error("", e);
		}
	}

	@Subscribe
	public void onDelete(DeleteEvent deleteEvent) {

		try {

			BigInteger id = dHash.getIdentifier().getValue();
			String hostName = ((InetSocketAddress) dHash.getServerInfo().getAddress()).getHostName();
			BigInteger key = deleteEvent.getKey().getValue();
			Object value = Cerealizer.unpack(deleteEvent.getEntry().getBytes());

			if (value instanceof ChordSet) {
				value = ((ChordSet) value).iterator().next();
			}

			String json = "{\n"
					+ "\"type\":\"delete\",\n"
					+ "\"id\":" + id + ",\n"
					+ "\"hostname\":\"" + hostName + "\",\n"
					+ "\"hash\":" + key + ",\n"
					+ "\"value\":" + value + "\n"
					+ "}";

			log.debug("deleteEvent = {}", json);
			publish(json);

		} catch (IOException e) {
			log.error("", e);
		}
	}

	@Subscribe
	public void onStatus(TripleStoreVisualizationChordStatus tripleStoreVisualizationChordStatus) {

		try {

			log.debug("status = {}", tripleStoreVisualizationChordStatus);
			publish(tripleStoreVisualizationChordStatus.toString());

		} catch (IOException e) {
			log.error("", e);
		}
	}

	private void publish(final String message) throws IOException {
		channel.basicPublish(config.brokerExchangeName, config.brokerRoutingKey, null, message.getBytes());
	}

	@Override
	protected void doStart() {

		try {

			final ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setHost(config.brokerHost);
			connectionFactory.setPort(config.brokerPort);
			connectionFactory.setUsername(config.brokerUsername);
			connectionFactory.setPassword(config.brokerPassword);
			connectionFactory.setVirtualHost(config.brokerVirtualHost);

			connection = connectionFactory.newConnection();
			channel = connection.createChannel();

			channel.exchangeDeclare(
					config.brokerExchangeName,
					config.brokerExchangeType
			);

			channel.queueDeclare(
					config.brokerQueueName,
					config.brokerQueueDurable,
					config.brokerQueueExclusive,
					config.brokerQueueAutoDelete,
					null
			);

			SingletonEventBus.getEventBus().register(this);

			notifyStarted();

		} catch (IOException e) {
			notifyFailed(e);
		}
	}

	@Override
	protected void doStop() {

		try {

			channel.close();
			connection.close();

			notifyStopped();

		} catch (IOException e) {
			notifyFailed(e);
		}
	}
}