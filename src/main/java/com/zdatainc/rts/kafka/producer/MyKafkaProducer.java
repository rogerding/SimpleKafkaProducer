package com.zdatainc.rts.kafka.producer;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Properties;
import java.nio.charset.Charset;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MyKafkaProducer implements Runnable
{
    private static final Logger LOGGER = Logger.getLogger(MyKafkaProducer.class);
    private final Charset ENC = Charset.forName("UTF-8");
    private final String topic;

    private InputStream inputStream = null;

    public MyKafkaProducer(String topic, InputStream stream)
    {
        this.topic = topic;
        this.inputStream = stream;
    }

    public void run()
    {
        // The first step in your code is to define properties for how the Producer finds the cluster,
        // serializes the messages and if appropriate directs the message to a specific Partition.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        ProducerConfig conf = new ProducerConfig(props);

        Producer<Integer, String> producer = null;
        BufferedReader rd = null;
        try
        {
            try
            {
                // Creates a buffering character-input stream that uses a default-sized input buffer.
                rd = new BufferedReader(
                        // Creates an InputStreamReader that uses the given charset.
                        new InputStreamReader(this.inputStream, ENC)
                );
                String line = null;

                LOGGER.debug("Producing messages");

                // Next you define the Producer object itself:
                // Note that the Producer is a Java Generic and you need to tell it the type of two parameters.
                // The first is the type of the Partition key, the second the type of the message
                producer = new Producer<Integer, String>(conf);

                // Reads a line of text.
                while ((line = rd.readLine()) != null)
                {
                    producer.send(
                        // build your message:
                        // def this(topic : scala.Predef.String, message : V)
                        //def this(topic : scala.Predef.String, key : K, message : V) = { /* compiled code */ }
                        // No key is used here
                        // if you do not include a key, even if you've defined a partitioner class,
                        // Kafka will assign the message to a random partition.
                        new KeyedMessage<Integer, String>(this.topic, line)
                    );
                }
                LOGGER.debug("Done sending messages");
            }
            catch (IOException ex)
            {
                LOGGER.fatal("IO Error while producing messages", ex);
                LOGGER.trace(null, ex);
            }
        }
        catch (Exception ex)
        {
            LOGGER.fatal("Error while producing messages", ex);
            LOGGER.trace(null, ex);
        }
        finally
        {
            try
            {
                if (rd != null) rd.close();
                if (producer != null) producer.close();
            }
            catch (IOException ex)
            {
                LOGGER.fatal("IO error while cleaning up", ex);
                LOGGER.trace(null, ex);
            }
        }
    }
}
