package com.enjoy.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfManager {

    private static final Logger logger = LoggerFactory.getLogger(ConfManager.class);

    private static Properties prop = new Properties();
    static {
        logger.info("init config file");
        InputStream in = ConfManager.class.getClassLoader().getResourceAsStream("conf.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String getStrValue(String key) {

        if (key.length() == 0 || key.equals("")) {
            logger.error("the input argument is empty");
            throw new IllegalArgumentException("the input key is empty");
        }

        return prop.getProperty(key);
    }

    public static int getIntValue(String key) {

        if (key.length() == 0 || key.equals("")) {
            logger.error("the input argument is empty");
            throw new IllegalArgumentException("the input key is empty");
        }

        if (key.matches("^\\d+$")) {
            // fix if value > Integer.MAX_VALUE 就要用Long 来valueOf了
            return Integer.valueOf(prop.getProperty(key));
        }
        logger.error("the value of the " + key + "can format into numeric");
        throw new NumberFormatException("the value of the " + key + "can format into numeric");
    }


}
