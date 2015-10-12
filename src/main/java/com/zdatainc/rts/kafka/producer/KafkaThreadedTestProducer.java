package com.zdatainc.rts.kafka.producer;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class KafkaThreadedTestProducer
{
    private static final Logger LOGGER =
        Logger.getLogger(KafkaThreadedTestProducer.class);
    private static final int BUFFER_LEN = 4096;
    public static void main(String[] args)
    {
        if (args.length < 2)
            throw new RuntimeException("Not enough arguments were passed");
        BasicConfigurator.configure();
        for (int i = 1; i < args.length; i++)
        {
            produceForFile(args[0], args[i]);
        }
    }

    private static void produceForFile(String topic, String filename)
    {
        try
        {
            LOGGER.debug("Setting up streams");

            // Creates a PipedInputStream so that it is not yet connected and uses the specified pipe size for the pipe's buffer.
            PipedInputStream send = new PipedInputStream(BUFFER_LEN);
            // Creates a piped output stream connected to the specified piped input stream.
            // Here: connect fileReaderOutput to Kafka's send
            PipedOutputStream fileReaderOutput = new PipedOutputStream(send);

            LOGGER.debug("Setting up connections");
            LOGGER.debug("Setting up file reader");
            // BufferedFileReader's output is Kafka's input
            BufferedFileReader reader = new BufferedFileReader(filename, fileReaderOutput);

            LOGGER.debug("Setting up kafka producer");
            MyKafkaProducer myKafkaProducer = new MyKafkaProducer(topic, send);

            LOGGER.debug("Spinning up threads");
            Thread source = new Thread(reader);
            Thread kafka = new Thread(myKafkaProducer);

            source.start();
            kafka.start();

            LOGGER.debug("Joining");
            kafka.join();
        }
        catch (IOException ex)
        {
            LOGGER.fatal("IO Error while piping", ex);
            LOGGER.trace(null, ex);
        }
        catch (InterruptedException ex)
        {
            LOGGER.warn("interruped", ex);
            LOGGER.trace(null, ex);
        }
    }
}
