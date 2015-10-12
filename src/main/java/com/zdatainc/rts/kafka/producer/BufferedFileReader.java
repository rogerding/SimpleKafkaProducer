package com.zdatainc.rts.kafka.producer;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

public class BufferedFileReader implements Runnable
{
    private final Logger LOGGER = Logger.getLogger(BufferedFileReader.class);
    private final Charset ENC = Charset.forName("UTF-8");
    private OutputStream outputStream = null;
    private String fileToRead;

    public BufferedFileReader(String file, OutputStream stream)
    {
        this.fileToRead = file;
        this.outputStream = stream;
    }

    public void run()
    {
        BufferedReader rd = null;
        BufferedWriter wd = null;
        try
        {
            try
            {
                // Creates a buffering character-input stream that uses a default-sized input buffer.
                rd = new BufferedReader(new FileReader(this.fileToRead));
                // Creates a buffered character-output stream that uses a default-sized output buffer.
                // BufferedWriter(Writer out)
                wd = new BufferedWriter(
                        // An OutputStreamWriter is a bridge from character streams to byte streams:
                        // Characters written to it are encoded into bytes using a specified charset.
                        // Creates an OutputStreamWriter that uses the given charset.
                        // OutputStreamWriter(OutputStream out, Charset cs)
                        new OutputStreamWriter(this.outputStream, ENC)
                );
                int b = -1;
                LOGGER.debug("Reading stream");

                // Reads a single character.
                // Returns: The character read, as an integer in the range 0 to 65535 (0x00-0xffff),
                // or -1 if the end of the stream has been reached
                while ((b = rd.read()) != -1)
                {
                    wd.write(b);
                }
                LOGGER.debug("Finished reading");
            }
            catch (FileNotFoundException ex)
            {
                LOGGER.fatal("Tried to read a file that does not exist", ex);
                LOGGER.trace("", ex);
            }
            catch (IOException ex)
            {
                LOGGER.fatal("IO Error while reading file", ex);
                LOGGER.trace("", ex);
            }
            finally
            {
                try
                {
                    if (rd != null) rd.close();
                    if (wd != null) wd.close();
                }
                catch (IOException ex)
                {
                    LOGGER.fatal("IO Error while cleaning up", ex);
                    LOGGER.trace(null, ex);
                }
            }
        }
        catch (Exception ex)
        {
            LOGGER.fatal("Error while reading file", ex);
            LOGGER.trace("", ex);
        }
    }
}
